package main

import (
	"fmt"
	"os"
	"time"

	log "github.com/sirupsen/logrus"

	"github.com/araddon/dateparse"
	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/aws/session"
	"github.com/aws/aws-sdk-go/service/s3"
	"github.com/aws/aws-sdk-go/service/s3/s3iface"
	"gopkg.in/urfave/cli.v1"
)

func setVersion(svc s3iface.S3API, bucket string, key string, versionId string) error {
	log.Info("Setting version: ", bucket, " ", key, " ", versionId)
	if versionId == "DELETE" {
		input := &s3.DeleteObjectInput{
			Bucket: &bucket,
			Key:    &key,
		}
		_, err := svc.DeleteObject(input)
		if err != nil {
			return err
		}
	} else {
		src := "/" + bucket + "/" + key + "?versionId=" + versionId
		input := &s3.CopyObjectInput{
			CopySource: &src,
			Bucket:     &bucket,
			Key:        &key,
		}

		_, err := svc.CopyObject(input)
		if err != nil {
			return err
		}
	}
	return nil
}

func makeDeleteMarker(obj *s3.ObjectVersion) s3.ObjectVersion {
	fakeVersionID := "DELETE"
	return s3.ObjectVersion{
		Key:          obj.Key,
		LastModified: obj.LastModified,
		VersionId:    &fakeVersionID,
	}
}

func makeDeleteMarker2(obj *s3.DeleteMarkerEntry) s3.ObjectVersion {
	fakeVersionID := "DELETE"
	return s3.ObjectVersion{
		Key:          obj.Key,
		LastModified: obj.LastModified,
		VersionId:    &fakeVersionID,
	}
}
func processVersion(final *map[string]s3.ObjectVersion, target time.Time, obj *s3.ObjectVersion) {
	log.Print("Processing: ", obj)

	key := *obj.Key
	curObj, exists := (*final)[key]
	log.Info("Key ", key, " exists ", exists)

	if !exists {
		log.Info("Adding key that doesn't exist", key)
		// insert a delete marker for things that didn't exist
		if obj.LastModified.After(target) {
			(*final)[key] = makeDeleteMarker(obj)
			return
		}
		(*final)[key] = *obj
		return
	}
	if obj.LastModified.After(target) {
		return
	}
	// This is our object if
	// If it is newer than what is currently in place
	// b) curObj is currently a future delete marker
	if obj.LastModified.After(*(*final)[key].LastModified) || curObj.LastModified.After(target) {
		(*final)[key] = *obj
	}
}

func processDeleteMarker(final *map[string]s3.ObjectVersion, target time.Time, obj *s3.DeleteMarkerEntry) {
	if obj.LastModified.After(target) {
		return
	}
	key := *obj.Key
	curObj, exists := (*final)[key]
	if obj.LastModified.After(target) {
		return
	}

	if !exists || obj.LastModified.After(*(*final)[key].LastModified) || curObj.LastModified.After(target) {
		(*final)[key] = makeDeleteMarker2(obj)
	}
}

func buildVersionDictionary(svc s3iface.S3API, bucket string, target time.Time) (final map[string]s3.ObjectVersion, err error) {
	final = make(map[string]s3.ObjectVersion)
	log.Print("Getting versions...")
	err = svc.ListObjectVersionsPages(&s3.ListObjectVersionsInput{
		Bucket: &bucket,
	}, func(p *s3.ListObjectVersionsOutput, last bool) (shouldContinue bool) {
		for _, obj := range p.Versions {
			processVersion(&final, target, obj)
		}
		for _, obj := range p.DeleteMarkers {
			processDeleteMarker(&final, target, obj)
		}
		return true
	})
	if err != nil {
		fmt.Println("failed to list objects", err)
		return
	}
	return
}

func processDictionary(svc s3iface.S3API, bucket string, final map[string]s3.ObjectVersion) error {
	log.Info("Processing final map", final)
	for key, value := range final {
		err := setVersion(svc, bucket, key, *value.VersionId)
		if err != nil {
			return err
		}
	}
	return nil
}

func process(c *cli.Context) error {
	fmt.Println("Beginning processing")
	endpointurl := c.GlobalString("endpoint-url")
	awsConfig := &aws.Config{
		Endpoint: &endpointurl,
	}

	sess := session.Must(session.NewSession(awsConfig))
	if sess == nil {
		fmt.Println("Failed to create aws session")
	}
	svc := s3.New(sess, awsConfig)
	if c.GlobalString("bucket") == "" {
		fmt.Println("No bucket specified")
		return nil
	}
	targetTime, err := dateparse.ParseAny(c.GlobalString("time"))
	log.Print("Parsed time ", targetTime)
	if err != nil {
		fmt.Println("Invalid time string", err)
		return err
	}
	objects, err := buildVersionDictionary(svc, c.GlobalString("bucket"), targetTime)
	if err != nil {
		fmt.Println("Failed building dictionary", err)
		return err
	}
	fmt.Println("Found ", len(objects), " keys")
	err = processDictionary(svc, c.GlobalString("bucket"), objects)
	if err != nil {
		fmt.Println("Failed processing dictionary", err)
	}
	fmt.Println("Done")
	return err
}
func init() {
	log.SetOutput(os.Stderr)

	// Only log the warning severity or above.
	log.SetLevel(log.ErrorLevel)
}

func main() {
	app := cli.NewApp()
	app.Flags = []cli.Flag{
		cli.StringFlag{
			Name:  "bucket",
			Usage: "required: s3 bucket to process",
		},
		cli.StringFlag{
			Name:  "time",
			Usage: "required: time to restore to. Use format: 2006-01-02T15:04:05.999999999Z07:00",
		},
		cli.StringFlag{
			Name:  "endpoint-url",
			Usage: "S3 compatible endpoint url",
		},
		cli.BoolFlag{
			Name:  "debug",
			Usage: "Use to enable debug logging",
		},
	}

	app.Commands = []cli.Command{

		{
			Name:   "restore",
			Usage:  "restore the s3 bucket to the time specified",
			Action: process,
		},
	}
	log.Print("Running App..")

	app.Before = func(c *cli.Context) error {
		if c.Bool("debug") {
			log.SetLevel(log.DebugLevel)
		}
		return nil
	}

	app.Run(os.Args)
}

package main

import (
	"fmt"
	"time"

	"github.com/araddon/dateparse"
	"github.com/aws/aws-sdk-go/aws/session"
	"github.com/aws/aws-sdk-go/service/s3"
	"gopkg.in/urfave/cli.v1"
)

func setVersion(svc *s3.S3, bucket string, key string, versionId string) error {
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
		src := key + "?versionId=" + versionId
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

func processVersion(final *map[string]s3.ObjectVersion, target time.Time, obj *s3.ObjectVersion) {
	if obj.LastModified.After(target) {
		return
	}
	key := *obj.Key
	curObj, exists := (*final)[key]

	if !exists {
		(*final)[key] = *obj
		return
	}

	if curObj.LastModified.After(*(*final)[key].LastModified) {
		(*final)[key] = *obj
	}
}

func processDeleteMarker(final *map[string]s3.ObjectVersion, target time.Time, obj *s3.DeleteMarkerEntry) {
	if obj.LastModified.After(target) {
		return
	}
	fakeVersionID := "DELETE"
	key := *obj.Key
	curObj, exists := (*final)[key]
	deleteObj := s3.ObjectVersion{
		Key:          obj.Key,
		LastModified: obj.LastModified,
		VersionId:    &fakeVersionID,
	}

	if !exists {
		(*final)[key] = deleteObj
		return
	}

	if curObj.LastModified.After(*(*final)[key].LastModified) {
		(*final)[key] = deleteObj
	}
}

func buildVersionDictionary(svc *s3.S3, bucket string, target time.Time) (final map[string]s3.ObjectVersion, err error) {
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

func processDictionary(svc *s3.S3, bucket string, final map[string]s3.ObjectVersion) error {
	for key, value := range final {
		err := setVersion(svc, bucket, key, *value.VersionId)
		if err != nil {
			return err
		}
	}
	return nil
}

func process(c *cli.Context) error {
	sess := session.Must(session.NewSession())
	svc := s3.New(sess)
	targetTime, err := dateparse.ParseAny(c.GlobalString("time"))
	if err != nil {
		return fmt.Errorf("Failed parsing time string: " + c.GlobalString("taget") + ":" + err.Error())
	}
	objects, err := buildVersionDictionary(svc, c.GlobalString("bucket"), targetTime)
	if err != nil {
		return fmt.Errorf("Failed building dictionary of versions" + err.Error())
	}

	err = processDictionary(svc, c.GlobalString("bucket"), objects)
	return err
}

func main() {
	app := cli.NewApp()
	app.Flags = []cli.Flag{
		cli.StringFlag{
			Name:  "bucket",
			Usage: "s3 bucket to process",
		},
		cli.StringFlag{
			Name:  "time",
			Usage: "time to restore to. Use format: 2006-01-02T15:04:05.999999999Z07:00",
		},
	}

	app.Commands = []cli.Command{
		{
			Name:   "restore",
			Usage:  "restore the s3 bucket to the time specified",
			Action: process,
		},
	}
}

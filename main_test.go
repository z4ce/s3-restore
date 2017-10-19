package main

import (
	"testing"
	"time"

	"github.com/aws/aws-sdk-go/service/s3"
	"github.com/aws/aws-sdk-go/service/s3/s3iface"
	. "github.com/onsi/ginkgo"
	. "github.com/onsi/gomega"
)

func ptrTo(s string) *string {
	return &s
}

func ptrToI(s int64) *int64 {
	return &s
}

func ptrToB(s bool) *bool {
	return &s
}

func TestMain(t *testing.T) {
	RegisterFailHandler(Fail)
	RunSpecs(t, "Main")
}

// Define a mock struct to be used in your unit tests of myFunc.
type mockS3Client struct {
	CopySrc *string
	Bucket  *string
	Key     *string
	s3iface.S3API
}

func (m *mockS3Client) CopyObject(input *s3.CopyObjectInput) (*s3.CopyObjectOutput, error) {
	m.Key = input.Key
	m.Bucket = input.Bucket
	m.CopySrc = input.CopySource
	return nil, nil
}

func (m *mockS3Client) DeleteObject(input *s3.DeleteObjectInput) (*s3.DeleteObjectOutput, error) {
	m.Key = input.Key
	m.Bucket = input.Bucket
	return nil, nil
}

func (m *mockS3Client) ListObjectVersionsPages(input *s3.ListObjectVersionsInput, process func(p *s3.ListObjectVersionsOutput, last bool) (shouldContinue bool)) error {
	startTime := time.Now()
	startTimeMinusHour := startTime.Add(time.Hour * -1)
	output := s3.ListObjectVersionsOutput{
		Versions: []*s3.ObjectVersion{
			&s3.ObjectVersion{
				ETag:         ptrTo("\"6994d44ab6c3b4c005357798f6b0d750\""),
				IsLatest:     ptrToB(false),
				Key:          ptrTo("file2"),
				LastModified: &startTimeMinusHour,
				Owner: &s3.Owner{
					DisplayName: ptrTo("user"),
					ID:          ptrTo("ownerid"),
				},
				Size:         ptrToI(6),
				StorageClass: ptrTo("STANDARD"),
				VersionId:    ptrTo("versionid1"),
			},
			&s3.ObjectVersion{
				ETag:         ptrTo("\"6994d44ab6c3b4c005357798f6b0d750\""),
				IsLatest:     ptrToB(false),
				Key:          ptrTo("file1"),
				LastModified: &startTime,
				Owner: &s3.Owner{
					DisplayName: ptrTo("user"),
					ID:          ptrTo("ownerid"),
				},
				Size:         ptrToI(6),
				StorageClass: ptrTo("STANDARD"),
				VersionId:    ptrTo("versionid1"),
			},
		},
	}
	process(&output, true)
	return nil
}

var _ = Describe("Main", func() {
	It("deletes correctly", func() {
		final := make(map[string]s3.ObjectVersion)
		startTime := time.Now()
		startTimeMinusHour := startTime.Add(time.Hour * -1)
		startTimeMinus2Hour := startTimeMinusHour.Add(time.Hour * -1)

		final["file1"] = s3.ObjectVersion{
			Key:          ptrTo("file1"),
			LastModified: &startTimeMinus2Hour,
			VersionId:    ptrTo("1"),
		}

		marker := s3.DeleteMarkerEntry{
			Key:          ptrTo("file1"),
			VersionId:    ptrTo("2"),
			LastModified: &startTimeMinusHour,
		}

		processDeleteMarker(&final, startTime, &marker)
		Expect(*(final["file1"].VersionId)).To(Equal("DELETE"))
	})

	It("doesn't delete when it shouldn't", func() {
		final := make(map[string]s3.ObjectVersion)
		startTime := time.Now()
		startTimeMinusHour := startTime.Add(time.Hour * -1)
		startTimeMinus2Hour := startTimeMinusHour.Add(time.Hour * -1)
		file := s3.ObjectVersion{
			Key:          ptrTo("file1"),
			LastModified: &startTimeMinusHour,
			VersionId:    ptrTo("1"),
		}

		final["file1"] = file

		marker := s3.DeleteMarkerEntry{
			Key:          ptrTo("file1"),
			VersionId:    ptrTo("2"),
			LastModified: &startTimeMinus2Hour,
		}

		processDeleteMarker(&final, startTime, &marker)
		Expect(final["file1"]).To(Equal(file))
	})

	It("Processes a version when it should", func() {
		final := make(map[string]s3.ObjectVersion)
		startTime := time.Now()
		startTimeMinusHour := startTime.Add(time.Hour * -1)
		file := s3.ObjectVersion{
			Key:          ptrTo("file1"),
			LastModified: &startTimeMinusHour,
			VersionId:    ptrTo("1"),
		}
		processVersion(&final, startTime, &file)
		Expect(final["file1"]).To(Equal(file))
	})

	It("Creates a delete marker for future versions", func() {
		final := make(map[string]s3.ObjectVersion)
		startTime := time.Now()
		startTimeMinusHour := startTime.Add(time.Hour * -1)
		file := s3.ObjectVersion{
			Key:          ptrTo("file1"),
			LastModified: &startTime,
			VersionId:    ptrTo("1"),
		}
		processVersion(&final, startTimeMinusHour, &file)
		Expect(*(final["file1"].VersionId)).To(Equal("DELETE"))
	})

	It("Chooses the newest applicable version", func() {
		final := make(map[string]s3.ObjectVersion)
		startTime := time.Now()
		startTimeMinusHour := startTime.Add(time.Hour * -1)
		startTimeMinus2Hour := startTimeMinusHour.Add(time.Hour * -1)
		file := s3.ObjectVersion{
			Key:          ptrTo("file1"),
			LastModified: &startTimeMinus2Hour,
			VersionId:    ptrTo("1"),
		}
		filev2 := s3.ObjectVersion{
			Key:          ptrTo("file1"),
			LastModified: &startTimeMinusHour,
			VersionId:    ptrTo("2"),
		}
		processVersion(&final, startTime, &file)
		processVersion(&final, startTime, &filev2)
		Expect(final["file1"]).To(Equal(filev2))
	})

	It("Chooses the newest applicable version processed backward", func() {
		final := make(map[string]s3.ObjectVersion)
		startTime := time.Now()
		startTimeMinusHour := startTime.Add(time.Hour * -1)
		startTimeMinus2Hour := startTimeMinusHour.Add(time.Hour * -1)
		file := s3.ObjectVersion{
			Key:          ptrTo("file1"),
			LastModified: &startTimeMinus2Hour,
			VersionId:    ptrTo("1"),
		}
		filev2 := s3.ObjectVersion{
			Key:          ptrTo("file1"),
			LastModified: &startTimeMinusHour,
			VersionId:    ptrTo("2"),
		}

		processVersion(&final, startTime, &filev2)
		processVersion(&final, startTime, &file)

		Expect(final["file1"]).To(Equal(filev2))
	})

	It("Sets a version", func() {
		mockSvc := &mockS3Client{}
		setVersion(mockSvc, "bucket", "key", "versionid")
		Expect(*(mockSvc.Key)).To(Equal("key"))
		Expect(*(mockSvc.Bucket)).To(Equal("bucket"))
		Expect(*(mockSvc.CopySrc)).To(Equal("/bucket/key?versionId=versionid"))
	})

	It("Deletes when a version is marked", func() {
		mockSvc := &mockS3Client{}
		setVersion(mockSvc, "bucket", "key", "DELETE")
		Expect(*(mockSvc.Key)).To(Equal("key"))
		Expect(*(mockSvc.Bucket)).To(Equal("bucket"))
	})
	It("Processes a dictionary of versions", func() {
		startTime := time.Now()
		mockSvc := &mockS3Client{}
		dict, err := buildVersionDictionary(mockSvc, "bucket", startTime)
		Expect(err).To(BeNil())
		Expect(*(dict["file2"].VersionId)).To(Equal("versionid1"))
		Expect(*(dict["file1"].VersionId)).To(Equal("DELETE"))

	})
})

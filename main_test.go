package main

import (
	"testing"
	"time"

	"github.com/aws/aws-sdk-go/service/s3"
	. "github.com/onsi/ginkgo"
	. "github.com/onsi/gomega"
)

func ptrTo(s string) *string {
	return &s
}

func TestMain(t *testing.T) {
	RegisterFailHandler(Fail)
	RunSpecs(t, "Main")
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
})

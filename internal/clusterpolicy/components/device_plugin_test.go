package components

import (
	"testing"

	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"

	rblnv1beta1 "github.com/rebellions-sw/rbln-npu-operator/api/v1beta1"
)

func TestPatch(t *testing.T) {
	RegisterFailHandler(Fail)
	RunSpecs(t, "Patch Suite")
}

var _ = Describe("DevicePluginPatcher", func() {
	var patcher *devicePluginPatcher

	Describe("buildDevicePluginConfig", func() {
		Context("with valid resource list", func() {
			BeforeEach(func() {
				patcher = &devicePluginPatcher{
					desiredSpec: &rblnv1beta1.RBLNDevicePluginSpec{
						ResourceList: []rblnv1beta1.RBLNDevicePluginResourceSpec{
							{
								ResourceName:     "ATOM",
								ResourcePrefix:   "rebellions.ai",
								ProductCardNames: []string{"RBLN-CA22", "RBLN-CA25"},
							},
						},
					},
				}
			})

			It("should generate correct config JSON", func() {
				result, err := patcher.buildDevicePluginConfig()

				Expect(err).NotTo(HaveOccurred())
				Expect(result).To(ContainSubstring(`"resourceName": "ATOM"`))
				Expect(result).To(ContainSubstring(`"resourcePrefix": "rebellions.ai"`))
				Expect(result).To(ContainSubstring(`"deviceType": "accelerator"`))
				Expect(result).To(ContainSubstring(`"1220"`))
				Expect(result).To(ContainSubstring(`"1221"`))
				Expect(result).To(ContainSubstring(`"1250"`))
				Expect(result).To(ContainSubstring(`"1251"`))
				Expect(result).NotTo(ContainSubstring(`"1120"`))
				Expect(result).NotTo(ContainSubstring(`"1121"`))
			})
		})

		Context("with invalid product card name", func() {
			BeforeEach(func() {
				patcher = &devicePluginPatcher{
					desiredSpec: &rblnv1beta1.RBLNDevicePluginSpec{
						ResourceList: []rblnv1beta1.RBLNDevicePluginResourceSpec{
							{
								ResourceName:     "ATOM",
								ResourcePrefix:   "rebellions.ai",
								ProductCardNames: []string{"RBLN-CA12", "INVALID"},
							},
						},
					},
				}
			})

			It("should return error for invalid product card name", func() {
				_, err := patcher.buildDevicePluginConfig()
				Expect(err).To(HaveOccurred())
				Expect(err.Error()).To(ContainSubstring("unknown product card name: INVALID"))
			})
		})
	})
})

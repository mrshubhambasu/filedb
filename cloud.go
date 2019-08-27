package main

import (
	"bytes"
	"fmt"
	"io"
	"io/ioutil"
	"log"
	"mime/multipart"
	"net/http"
	"os"

	"github.com/gin-gonic/gin"

	"corelab.mkcl.org/MKCLOS/coredevelopmentplatform/corepkgv2/hashmdl"

	"github.com/tidwall/gjson"

	"github.com/tidwall/sjson"

	"strconv"
	"time"
	//_"corelab.mkcl.org\MKCLOS\coredevelopmentplatform\corepkgv2\securitymdl\securitymdl.go"
)

var Globfile string

func main() {
	start := time.Now()
	router := gin.Default()
	router.POST("/one", func(c *gin.Context) {
		// single file
		file, err := c.FormFile("file")
		if err != nil {
			log.Fatal(err)
		}
		log.Println(file.Filename)

		err = c.SaveUploadedFile(file, "cloud/"+file.Filename)
		if err != nil {
			log.Fatal(err)
		}
		c.String(http.StatusOK, fmt.Sprintf("'%s' uploaded!", file.Filename))
		MergePack("cloud/"+file.Filename, "packdb/pack.txt")
	})
	router.Run(":8062")
	// listen and serve on 0.0.0.0:8080

	// func fileUpload(c *gin.Context) {x`
	// 	var person Person
	// 	if c.ShouldBindQuery(&person) == nil {
	// 		log.Println("====== Only Bind By Query String ======")
	// 		log.Println(person.Name)
	// 		log.Println(person.Address)
	// 	}
	// 	c.String(200, "Success")
	// }
	files := []string{"packdb/a.txt", "packdb/b.txt", "packdb/c.txt", "packdb/d.txt"}
	fmt.Println(files)

	// creating new/initial pack file
	// CreatePackFile(files, "packdb/pack.txt")

	// creating updated pack file
	CreateUpdatedPack(files, "packdb/pack.txt", "packdb/pack2.txt")

	// merging updated pack file with main pack file
	// MergePack("packdb/pack2.txt", "packdb/pack.txt")

	// GetFileDataListFromPack("packdb/pack.txt", files)

	elapsed := time.Now().Sub(start)
	log.Println(elapsed.Nanoseconds())
	log.Println(elapsed)

}

func fetchIndexTable(packFilePath string) (string, int64) {
	f, err := os.Open(packFilePath)
	check(err)
	_, err = f.Seek(0, 0)
	check(err)
	indexData := make([]byte, 15)
	_, err = f.Read(indexData)
	check(err)
	// fmt.Println("",indexData)
	// fmt.Println("indexDataStartHeader", string(indexData))
	indexDataStartHeader := string(indexData)
	startOffset, err := strconv.ParseInt(indexDataStartHeader, 10, 64)
	// fmt.Println("Start", indexDataStartHeader, startOffset, err)
	_, err = f.Seek(15, 0)
	check(err)
	indexSize := make([]byte, 15)
	_, err = f.Read(indexSize)
	check(err)
	indexDataSizeHeader := string(indexSize)

	// strconv.ParseInt(s string, base int, bitSize int)

	start, _ := strconv.Atoi(indexDataStartHeader)
	size, _ := strconv.Atoi(indexDataSizeHeader)
	// fmt.Println(start, size)
	_, err = f.Seek(int64(start), 0)
	metaByte := make([]byte, size)
	_, err = f.Read(metaByte)
	return string(metaByte), startOffset
}

func getFileSizeFromPack(data string, name string) int64 {
	fmt.Println("Search bytesize", name)
	sizeOfFile := gjson.Get(data, `#[Filename=`+name+`].allocation.bytesize`)

	return (sizeOfFile.Int())
}

func getFileStartOffsetFromPack(data string, name string) int64 {
	fmt.Println("Search startoffset", name)
	startoffset := gjson.Get(data, `#[Filename=`+name+`].allocation.startoffset`)

	return (startoffset.Int())
}

func getFileHashTextFromPack(data string, name string) string {
	fmt.Println("Search hashText", name)
	hashText := gjson.Get(data, `#[Filename=`+name+`].allocation.hashText`)

	return hashText.String()
}

// GetFileDataFromPack - GetFileDataFromPack
func GetFileDataFromPack(packPath string, fileName string) []byte {
	f, err := os.Open(packPath)
	check(err)
	_, err = f.Seek(0, 0)
	check(err)
	indexData := make([]byte, 15)
	_, err = f.Read(indexData)
	check(err)
	indexDataStartHeader := string(indexData)
	_, err = f.Seek(15, 0)
	check(err)
	indexSize := make([]byte, 15)
	_, err = f.Read(indexSize)
	check(err)
	indexDataSizeHeader := string(indexSize)

	start, _ := strconv.Atoi(indexDataStartHeader)
	size, _ := strconv.Atoi(indexDataSizeHeader)
	_, err = f.Seek(int64(start), 0)
	bytesOfIndexing := make([]byte, size)
	_, err = f.Read(bytesOfIndexing)

	// read data from pack
	sizeOfFile := gjson.Get(string(bytesOfIndexing), `#[Filename=`+fileName+`].allocation.bytesize`)
	startRead := gjson.Get(string(bytesOfIndexing), `#[Filename=`+fileName+`].allocation.startoffset`)
	_, err = f.Seek(startRead.Int(), 0)
	bytesOfFile := make([]byte, sizeOfFile.Int())
	_, err = f.Read(bytesOfFile)
	return bytesOfFile
}

// GetFileDataListFromPack - GetFileDataListFromPack
func GetFileDataListFromPack(packPath string, fileNameList []string) {
	f, err := os.Open(packPath)
	check(err)
	_, err = f.Seek(0, 0)
	check(err)
	indexData := make([]byte, 15)
	_, err = f.Read(indexData)
	check(err)
	indexDataStartHeader := string(indexData)
	_, err = f.Seek(15, 0)
	check(err)
	indexSize := make([]byte, 15)
	_, err = f.Read(indexSize)
	check(err)
	indexDataSizeHeader := string(indexSize)

	start, _ := strconv.Atoi(indexDataStartHeader)
	size, _ := strconv.Atoi(indexDataSizeHeader)
	_, err = f.Seek(int64(start), 0)
	bytesOfIndexing := make([]byte, size)
	_, err = f.Read(bytesOfIndexing)

	for ind := 0; ind < len(fileNameList); ind++ {
		sizeOfFile := gjson.Get(string(bytesOfIndexing), `#[Filename=`+fileNameList[ind]+`].allocation.bytesize`)
		startRead := gjson.Get(string(bytesOfIndexing), `#[Filename=`+fileNameList[ind]+`].allocation.startoffset`)
		_, err = f.Seek(startRead.Int(), 0)
		bytesOfFile := make([]byte, sizeOfFile.Int())
		_, err = f.Read(bytesOfFile)
		fmt.Println(string(bytesOfFile))
	}

}

func check(e error) {
	if e != nil {
		// fmt.Println("check - ", e)
	}
}

func appendPaddingToNumber(value int64) string {
	return fmt.Sprintf("%015d", value)
}

func wrapUpSession(endOffset int64, indexingData []byte, packCreationPath string) {
	f, err := os.OpenFile(packCreationPath,
		os.O_CREATE|os.O_WRONLY, 0644)
	if err != nil {
		// fmt.Println(err)
	}
	defer f.Close()
	//securitymdl.AESEncrypt()
	f.WriteAt(indexingData, endOffset)
	f.WriteAt([]byte(appendPaddingToNumber(endOffset)), 0)
	// fmt.Println("endOffset", endOffset, "appendPaddingToNumber(endOffset)", appendPaddingToNumber(endOffset))
	f.WriteAt([]byte(appendPaddingToNumber(int64(len(indexingData)))), 15)
}

// CreateUpdatedPack - This create an new pack by checking updated contents
func CreateUpdatedPack(filePathArray []string, packFilePath string, destinationPackFilePath string) {
	// fetch indexTableRecords and it's startOffset from pack file
	indexTableRecords, indexTableStartOffset := fetchIndexTable(packFilePath)
	fmt.Println("Original", indexTableStartOffset, indexTableRecords)

	listOfUpdatedFiles := []string{}

	for index := 0; index < len(filePathArray); index++ {
		// read data of given file
		newFileData, readErr := ioutil.ReadFile(filePathArray[index])
		check(readErr)

		// calculate hash of source file content
		newHashText := getHashOfFile(newFileData)

		// fetch hashText of given file in pack file
		existingFileHashText := getFileHashTextFromPack(indexTableRecords, filePathArray[index])

		fmt.Println("Hash", newHashText, existingFileHashText)

		// compare is file hash with existing record in pack file
		if newHashText != existingFileHashText {
			listOfUpdatedFiles = append(listOfUpdatedFiles, filePathArray[index])
		}
	}

	fmt.Println("Different Files", listOfUpdatedFiles)

	if len(listOfUpdatedFiles) == 0 {
		// no changes to create pack
		return
	}

	// create new pack with only changed content
	CreatePackFile(listOfUpdatedFiles, destinationPackFilePath)
}

// MergePack - MergePack
func MergePack(sourcePackFilePath string, destinationPackFilePath string) {
	// open destination pack file to read
	f, err := os.OpenFile(destinationPackFilePath,
		os.O_CREATE|os.O_WRONLY, 0644)
	check(err)
	defer f.Close()

	// fetch records from sourcePackFilePath
	sourcePackIndexTableRecords, sourcePackIndexTableStartOffset := fetchIndexTable(sourcePackFilePath)
	fmt.Println("New", sourcePackIndexTableRecords, sourcePackIndexTableStartOffset)

	//fetch All Filenames from sourcePackFilePath
	fileListFromSourcePack := gjson.Get(sourcePackIndexTableRecords, "#.Filename").Array()
	fmt.Println("Values", fileListFromSourcePack)

	// fetch indexTableRecords and it's startOffset from pack file
	destinationPackIndexTableRecords, destinationPackIndexTableStartOffset := fetchIndexTable(destinationPackFilePath)
	fmt.Println("Old", destinationPackIndexTableStartOffset, destinationPackIndexTableRecords)

	for index := 0; index < len(fileListFromSourcePack); index++ {
		// read data of given file
		newFileData := GetFileDataFromPack(sourcePackFilePath, fileListFromSourcePack[index].String())

		// calculate length of source file content
		newFileSize := int64(len(newFileData))

		// create hash for file content
		hashText := getHashOfFile(newFileData)

		// fetch existing file size from pack file
		existingFileSize := getFileSizeFromPack(destinationPackIndexTableRecords, fileListFromSourcePack[index].String())

		// fetch startOffset of given file in pack file if exist (If not then it is new file)
		existingFileStartOffset := getFileStartOffsetFromPack(destinationPackIndexTableRecords, fileListFromSourcePack[index].String())

		// compare is file smaller or larger than existing record in pack file
		if newFileSize <= existingFileSize {
			fmt.Println("Small File", destinationPackIndexTableStartOffset, newFileSize, existingFileSize)

			fmt.Println("Current Positon", existingFileStartOffset)
			fmt.Println("Existing Length", newFileSize)

			// write changed content to file
			f.WriteAt(newFileData, existingFileStartOffset)

			// update indext table with updated changes
			destinationPackIndexTableRecords = updateJSON(destinationPackIndexTableRecords, fileListFromSourcePack[index].String(), existingFileStartOffset, newFileSize, hashText)

			fmt.Println("jsonz2", destinationPackIndexTableRecords)
		} else {
			fmt.Println("Large File", destinationPackIndexTableStartOffset, newFileSize, existingFileSize)

			fmt.Println("Current Positon", existingFileStartOffset)
			fmt.Println("Existing Length", newFileSize)

			// write changed content to file
			f.WriteAt(newFileData, destinationPackIndexTableStartOffset)

			// check it is existing file or new file
			if existingFileStartOffset == 0 {
				// append new record to indext table
				destinationPackIndexTableRecords = createNewIndexRecord(destinationPackIndexTableRecords, fileListFromSourcePack[index].String(), destinationPackIndexTableStartOffset, newFileSize, hashText)
			} else {
				// update indext table with updated changes
				destinationPackIndexTableRecords = updateJSON(destinationPackIndexTableRecords, fileListFromSourcePack[index].String(), destinationPackIndexTableStartOffset, newFileSize, hashText)
			}

			// increment startOffset for moving index table ahead
			destinationPackIndexTableStartOffset += newFileSize

			fmt.Println("jsonz2", destinationPackIndexTableRecords)
		}
	}

	// write final updated index table to pack file
	wrapUpSession(destinationPackIndexTableStartOffset, []byte(destinationPackIndexTableRecords), destinationPackFilePath)
}

func updateJSON(jsonData string, name string, startOffset int64, size int64, hashText string) string {
	res := gjson.Parse(jsonData)
	var updatedRes []interface{}

	res.ForEach(func(key, value gjson.Result) bool {
		// fmt.Println("val:", value.Get("Filename").String())
		if value.Get("Filename").String() == name {
			// Update byte size
			updateValue, err := sjson.Set(value.String(), "allocation.bytesize", size)
			if err != nil {
				fmt.Println(err)
				return false
			}
			// Update startOffset
			updateValue, err = sjson.Set(updateValue, "allocation.startoffset", startOffset)
			if err != nil {
				fmt.Println(err)
				return false
			}
			// Update hashText
			updateValue, err = sjson.Set(updateValue, "allocation.hashText", hashText)
			if err != nil {
				fmt.Println(err)
				return false
			}
			updatedRes = append(updatedRes, gjson.Parse(updateValue).Value())
		} else {
			updatedRes = append(updatedRes, value.Value())
		}
		return true // keep iterating
	})
	temp := `{}`
	updateValue, err := sjson.Set(temp, "writeData", updatedRes)
	if err != nil {
		fmt.Print(err)
	}
	fileData := gjson.Parse(updateValue)
	//fmt.Println("FinalResult:", fileData.Get("writeData").String())

	return fileData.Get("writeData").String()

	//fmt.Println("FInal result:", updatedRes)
}

// CreatePackFile - CreatePackFile
func CreatePackFile(filePathArray []string, packCreationPath string) {
	// open pack file to read
	f, err := os.OpenFile(packCreationPath,
		os.O_CREATE|os.O_WRONLY, 0644)
	check(err)
	defer f.Close()

	f.WriteAt([]byte("HHHHHHHHHHHHHHHHHHHHHHHHHHHHHH"), 0)
	indexTableStartOffset := int64(30)

	indexTableRecords := "[]"
	for index := 0; index < len(filePathArray); index++ {
		// read data of given file
		newFileData, readErr := ioutil.ReadFile(filePathArray[index])
		check(readErr)

		newFileSize := int64(len(newFileData))

		// create hash for file content
		hashText := getHashOfFile(newFileData)

		// write content to file
		f.WriteAt((newFileData), indexTableStartOffset)

		// append new record to indext table
		indexTableRecords = createNewIndexRecord(indexTableRecords, filePathArray[index], indexTableStartOffset, newFileSize, hashText)

		// increment startOffset with file size
		indexTableStartOffset += newFileSize
	}

	// write final updated index table to pack file
	wrapUpSession(indexTableStartOffset, []byte(indexTableRecords), packCreationPath)
}

func getHashOfFile(data []byte) string {
	// create uint64 hash for file content
	hashOfSourceData, err := hashmdl.GetHashChecksumOfByteArray(data)
	check(err)

	// convert uint64 hash to string
	hashText := strconv.FormatUint(hashOfSourceData, 10)
	return hashText
}

func createNewIndexRecord(indexTableRecords string, Filename string, startoffset int64, bytesize int64, hashText string) string {
	jsonzobj := `{}`
	jsonzobj, _ = sjson.Set(jsonzobj, "Filename", Filename)
	jsonzobj, _ = sjson.Set(jsonzobj, "allocation.startoffset", startoffset)
	jsonzobj, _ = sjson.Set(jsonzobj, "allocation.bytesize", bytesize)
	jsonzobj, _ = sjson.Set(jsonzobj, "allocation.hashText", hashText)
	//println(jsonzobj)
	parsedJsonzObj := gjson.Parse(jsonzobj)

	indexTableRecords, _ = sjson.Set(indexTableRecords, "-1", parsedJsonzObj.Value())
	// jsonz, _ = sjson.Set(jsonz, "-1", parsedJsonzObj.Value())
	return indexTableRecords
}

func Upload(url, file string) (err error) {
	// Prepare a form that you will submit to that URL.
	var b bytes.Buffer
	w := multipart.NewWriter(&b)
	// Add your image file
	f, err := os.Open(file)
	if err != nil {
		return
	}
	defer f.Close()
	fw, err := w.CreateFormFile("file", file)
	if err != nil {
		return
	}
	if _, err = io.Copy(fw, f); err != nil {
		return
	}

	// Add the other fields
	if fw, err = w.CreateFormField("key"); err != nil {
		return
	}
	if _, err = fw.Write([]byte("KEY")); err != nil {
		return
	}
	// Don't forget to close the multipart writer.
	// If you don't close it, your request will be missing the terminating boundary.
	w.Close()

	// Now that you have a form, you can submit it to your handler.
	req, err := http.NewRequest("POST", url, &b)
	if err != nil {
		return
	}
	// Don't forget to set the content type, this will contain the boundary.
	req.Header.Set("Content-Type", w.FormDataContentType())

	// Submit the request
	client := &http.Client{}
	res, err := client.Do(req)
	if err != nil {
		return
	}

	// Check the response
	if res.StatusCode != http.StatusOK {
		err = fmt.Errorf("bad status: %s", res.Status)
	}
	return
}

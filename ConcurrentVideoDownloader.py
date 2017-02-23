import urllib2
from concurrent.futures import ThreadPoolExecutor, as_completed

def blockWrite(urlHandle, fileHandle, blockSize):
	while True:
		buf = urlHandle.read(blockSize)
		if not buf:
			return
		fileHandle.write(buf)

def downloadByteRange(url, startByte, endByte, binFile, part=0):
	print 'started downloading part {}: {}-{} bytes'.format(part, startByte, endByte)
	req = urllib2.Request(url)
	req.add_header('Range', 'bytes=' + str(startByte) + '-' + str(endByte))
	urlHandle = urllib2.urlopen(req)
	with open(binFile, 'wb') as fileHandle:
		blockWrite(urlHandle, fileHandle, blockSize=8192)

def getContentSize(url):
	result = urllib2.urlopen(url)
	return int(result.headers['content-length'])

#assuming part files in the format part-{partnum}.bin
def combineBinPartFiles(input_dir, parts, output):
	if parts == 0:
		print "Can't combine 0 part files..."
		return
	with open(output, 'wb') as outfile:
		for part in range(parts):
			partfile = '{}\\part-{}.bin'.format(input_dir, part)
			with open(partfile, 'rb') as infile:
				outfile.write(infile.read())

def waitForFutures(futures):
	for future in as_completed(futures):
		print future.result()

def concurrentDownload(url, output_folder, parts=3):
	sizeFull = getContentSize(url)
	sizePart = sizeFull//parts

	pool = ThreadPoolExecutor(parts)
	futures = []
	#make async calls for (part-1) full buckets
	for i in range(parts-1):
		#starts from 0
		startByte = i*sizePart
		endByte = startByte + sizePart - 1
		partFileBin = '{}\\part-{}.bin'.format(output_folder, i)
		futures.append(pool.submit(downloadByteRange, url, startByte, endByte, partFileBin, i))
	#make call for last bucket
	futures.append(pool.submit(downloadByteRange, url, (parts-1)*sizePart, '', '{}\\part-{}.bin'.format(output_folder, parts-1), parts-1))
	waitForFutures(futures)
	combineBinPartFiles(output_folder, parts, '{}\\output.mp4'.format(output_folder))

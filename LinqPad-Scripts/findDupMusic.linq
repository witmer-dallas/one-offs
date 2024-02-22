<Query Kind="Program">
  <NuGetReference>TagLibSharp</NuGetReference>
  <Namespace>System.Collections.Concurrent</Namespace>
  <Namespace>System.Security.Cryptography</Namespace>
  <Namespace>System.Threading.Tasks</Namespace>
  <Namespace>TagLib</Namespace>
  <Namespace>TagLib.Aac</Namespace>
  <Namespace>TagLib.Aiff</Namespace>
  <Namespace>TagLib.Ape</Namespace>
  <Namespace>TagLib.Asf</Namespace>
  <Namespace>TagLib.Audible</Namespace>
  <Namespace>TagLib.Dsf</Namespace>
  <Namespace>TagLib.Flac</Namespace>
  <Namespace>TagLib.Gif</Namespace>
  <Namespace>TagLib.Id3v1</Namespace>
  <Namespace>TagLib.Id3v2</Namespace>
  <Namespace>TagLib.IFD</Namespace>
  <Namespace>TagLib.IFD.Entries</Namespace>
  <Namespace>TagLib.IFD.Makernotes</Namespace>
  <Namespace>TagLib.IFD.Tags</Namespace>
  <Namespace>TagLib.IIM</Namespace>
  <Namespace>TagLib.Image</Namespace>
  <Namespace>TagLib.Image.NoMetadata</Namespace>
  <Namespace>TagLib.Jpeg</Namespace>
  <Namespace>TagLib.Matroska</Namespace>
  <Namespace>TagLib.Mpeg</Namespace>
  <Namespace>TagLib.Mpeg4</Namespace>
  <Namespace>TagLib.MusePack</Namespace>
  <Namespace>TagLib.NonContainer</Namespace>
  <Namespace>TagLib.Ogg</Namespace>
  <Namespace>TagLib.Ogg.Codecs</Namespace>
  <Namespace>TagLib.Png</Namespace>
  <Namespace>TagLib.Riff</Namespace>
  <Namespace>TagLib.Tiff</Namespace>
  <Namespace>TagLib.Tiff.Arw</Namespace>
  <Namespace>TagLib.Tiff.Cr2</Namespace>
  <Namespace>TagLib.Tiff.Dng</Namespace>
  <Namespace>TagLib.Tiff.Nef</Namespace>
  <Namespace>TagLib.Tiff.Pef</Namespace>
  <Namespace>TagLib.Tiff.Rw2</Namespace>
  <Namespace>TagLib.WavPack</Namespace>
  <Namespace>TagLib.Xmp</Namespace>
</Query>

async void Main()
{
	var myDirectory = "Q:\\Users\\Dallas\\Music";
	var myDirectoryInfo = new DirectoryInfo(myDirectory);
	var myResults = new List<SearchResult>();
	var resultHashDictonary = new ConcurrentDictionary<string, List<string>>();


	BuildCorruptFileList(myDirectoryInfo);
	CorruptFiles.Count().Dump();
	//NonCorruptFiles = myDirectoryInfo.EnumerateFiles("*.*", SearchOption.AllDirectories).Where(f => !CorruptFiles.Any(cf => cf.FullName == f.FullName)).ToList();

	("Checking for Dups").Dump();
	int fileCount = 0;
	int totalFiles = NonCorruptFiles.Count();
	var pb = new Util.ProgressBar();
	pb.Dump();

	//await Task.WhenAll(NonCorruptFiles.Select(async file =>
	//{
	//	fileCount++;
	//	pb.Caption = "Checking file " + fileCount + " of " + totalFiles;
	//	pb.Fraction = (float)fileCount / totalFiles;
	//	await ProcessFileAsync(file.FullName, resultHashDictonary);			
	//}));

	int batchSize = 50; // Adjust the batch size as needed	
	int processedFiles = 0;

	var batches = NonCorruptFiles.Select((file, index) => new { Index = index, File = file })
							.GroupBy(x => x.Index / batchSize, x => x.File)
							.ToList();

	foreach (var batch in batches)
	{
		var tasks = batch.Select(async file =>
		{		
			await ProcessFileAsync(file.FullName, resultHashDictonary);

			fileCount++;
			pb.Caption = "Checking file " + fileCount + " of " + totalFiles;
			pb.Fraction = (float)fileCount / totalFiles;
		});
		await Task.WhenAll(tasks);		
	}

	resultHashDictonary.Dump("results");

	//	foreach (var file in NonCorruptFiles.Take(MyLimit))
	//	{	
	//		pb.Caption = "Checking file " + fileCount + " of " + totalFiles;
	//		pb.Fraction = (float)fileCount / totalFiles;
	//		fileCount += 1;
	//		
	//		
	//		SearchResult result = new SearchResult();
	//
	//		var dupFile = FindDuplicateFileInDirectory(myDirectoryInfo, file);
	//		if (dupFile != null)
	//		{
	//			if(!result.FoundDups.Any(x=> x.FullName == dupFile.FullName)) result.FoundDups.Add(dupFile);
	//			dupFile.FullName.Dump();
	//		}
	//
	//		myResults.Add(result);
	//	}
	//	
	//	myResults.Where(x=> x.FoundDups.Count() > 0).Count().Dump("Files with Dups");
}

static async Task ProcessFileAsync(string filePath, ConcurrentDictionary<string, List<string>> hashDictionary)
{
	const int maxRetryAttempts = 5;
        const int delayMilliseconds = 500;

        int retryCount = 0;
        bool success = false;

	while (!success && retryCount < maxRetryAttempts)
	{
		try
		{
			string hash = await GetFileHashAsync(filePath);
			hashDictionary.AddOrUpdate(hash, new List<string> { filePath }, (key, existingList) =>
			{
				existingList.Add(filePath);
				return existingList;
			});

			success = true;
		}
		catch (IOException)
		{
			// File is not accessible, wait and retry
			await Task.Delay(delayMilliseconds);
			retryCount++;
		}
	}
}

static async Task<string> GetFileHashAsync(string filePath)
{
	using (var sha256 = SHA256.Create())
	using (var stream = new FileStream(filePath, FileMode.Open, FileAccess.Read, FileShare.Read, 4096, true))
	{
		byte[] hash = await sha256.ComputeHashAsync(stream);
		return BitConverter.ToString(hash).Replace("-", "").ToLower();
	}
}

public async void BuildCorruptFileList(DirectoryInfo directoryToSearch)
{
	("Checking for Corrupt Files").Dump();
	var files = directoryToSearch.EnumerateFiles("*.*", SearchOption.AllDirectories);
	int fileCount = 0;
	int totalFiles = files.Count();
	var pb = new Util.ProgressBar();
	pb.Dump();
	


	await Task.WhenAll(files.ToList().Select(async f =>
	{
		fileCount += 1;
		pb.Caption = "File " + fileCount + " of " + totalFiles;
		pb.Fraction = (float)fileCount / totalFiles;
		
		await AddToCorruptListIfCorrupt(f);		
	}));


//	foreach (var file in files.Take(MyLimit))
//	{
//		pb.Caption = "File " + fileCount + " of " + totalFiles;
//		pb.Fraction = (float)fileCount / totalFiles;
//		fileCount += 1;
//
//		if (IsFileCorrupt(file))
//		{
//			CorruptFiles.Add(file);
//		}
//		else
//		{
//			NonCorruptFiles.Add(file);
//		}
//	}
}

public Task AddToCorruptListIfCorrupt(FileInfo file)
{
	var isCorrupt = IsFileCorrupt(file);
	if (isCorrupt)
	{
		CorruptFiles.Add(file);
	}
	else
	{
		NonCorruptFiles.Add(file);
	}
	
	return Task.FromResult(isCorrupt);
}

public int MyLimit => 100000;
public List<FileInfo> CorruptFiles { get; set; } = new();
public List<FileInfo> NonCorruptFiles { get; set; } = new();

public bool IsFileCorrupt(FileInfo fileToFind)
{
	TagLib.File tagFile1 = null;

	try
	{
		tagFile1 = TagLib.File.Create(fileToFind.FullName);
		tagFile1.Dispose();
	}
	catch (Exception ex)
	{
		return true;
	}

	return false;
}

public FileInfo FindDuplicateFileInDirectory(DirectoryInfo directoryToSearch, FileInfo fileToFind)
{
	//var files = directoryToSearch.EnumerateFiles("*.*", SearchOption.AllDirectories).Where(f => !CorruptFiles.Any(cf => cf.FullName == f.FullName)).ToList();
	FileInfo dupfile = null;

	foreach (var file in NonCorruptFiles)
	{
		if (file.FullName == fileToFind.FullName)
		{
			//same exact file
			continue;
		}


		if (FileCompare(file.FullName, fileToFind.FullName))
		{
			dupfile = file;
		}
	}

	return dupfile;
}

public class SearchResult
{
	public FileInfo FileToSearch { get; set; }
	public List<FileInfo> FoundDups { get; set; } = new();

	public bool IsCorrupt { get; set; }
}

// You can define other methods, fields, classes and namespaces here

private bool FileCompare(string file1, string file2)
{
	int file1byte;
	int file2byte;
	FileStream fs1;
	FileStream fs2;

	// Determine if the same file was referenced two times.
	if (file1 == file2)
	{
		// Return true to indicate that the files are the same.
		return true;
	}

	// Open the two files.
	fs1 = new FileStream(file1, FileMode.Open);
	fs2 = new FileStream(file2, FileMode.Open);

	// Check the file sizes. If they are not the same, the files
	// are not the same.
	if (fs1.Length != fs2.Length)
	{
		// Close the file
		fs1.Close();
		fs2.Close();

		// Return false to indicate files are different
		return false;
	}

	// Read and compare a byte from each file until either a
	// non-matching set of bytes is found or until the end of
	// file1 is reached.
	do
	{
		// Read one byte from each file.
		file1byte = fs1.ReadByte();
		file2byte = fs2.ReadByte();
	}
	while ((file1byte == file2byte) && (file1byte != -1));

	// Close the files.
	fs1.Close();
	fs2.Close();

	// Return the success of the comparison. "file1byte" is
	// equal to "file2byte" at this point only if the files are
	// the same.
	return ((file1byte - file2byte) == 0);
}
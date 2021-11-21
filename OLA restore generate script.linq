<Query Kind="Program" />

void Main()
{
	// Generate a database restore script from Ole Hallengren's Full backup solution.
	// https://ola.hallengren.com/
	//
	// Procedure:
	// 1. Find most recent FULL backup less than or equal to the specified date
	// 2. Look for any Differential backups; if found apply the latest DIFF backup less than or equal to the specified date
	// 3. Apply any TLOG backups up to the specified date 
	// 4. Take database out of recovery mode.

	string serverName = "K7";
	string dbName = "AdventureWorks";
	string olaRootBackupFolder = @"C:\temp\Backup\";

	// Point in time: (year, month, day, hours, minutes, seconds)
	//DateTime restorePointInTime = new DateTime(2021, 11, 18, 16, 00, 00);
	// ...or latest available
	DateTime restorePointInTime = DateTime.Now;

	var s = GenerateRestoreScript(serverName, dbName, olaRootBackupFolder, restorePointInTime);
	
	Console.WriteLine("USE [master]" + Environment.NewLine);	
	Console.WriteLine(s);
}

public string GenerateRestoreScript(string serverName, string dbName, string olaRootBackupFolder, DateTime restorePointInTime)
{
	var mostRecentBackup = GetMostRecentMatchingFullBackup(serverName, dbName, olaRootBackupFolder, restorePointInTime);
	if (mostRecentBackup == null)
		return "";
	string cmd = mostRecentBackup.GenerateRestoreStatement(dbName, null);

	var mostRecentDiffBackup = GetMostRecentMatchingDiffBackup(serverName, dbName, olaRootBackupFolder, restorePointInTime, mostRecentBackup.FileDate, out DateTime tlogsAfter);
	if (mostRecentDiffBackup == null)
	{
		Console.WriteLine("-- No diff backup matches the given point in time, will attempt to generate restore TLog chain...");
	}
	else
	{
		cmd += mostRecentDiffBackup.GenerateRestoreStatement(dbName, restorePointInTime);		
	}

	var tlogs = GetBackupFiles(serverName, dbName, olaRootBackupFolder, BackupType.TLOG);
	var tlogsToApply = tlogs.Where(x => x.FileDate <= restorePointInTime && x.FileDate >= tlogsAfter).ToList();
	foreach (var log in tlogsToApply)
	{
		cmd += log.GenerateRestoreStatement(dbName, restorePointInTime);
	}

	cmd += $"RESTORE DATABASE [{dbName}] WITH RECOVERY";
	
	return cmd;
}

public BackupFile GetMostRecentMatchingFullBackup(string serverName, string dbName, string olaRootBackupFolder, DateTime restorePointInTime)
{
	var fullbackups = GetBackupFiles(serverName, dbName, olaRootBackupFolder, BackupType.FULL);
	
	if (fullbackups.Count == 0)
	{
		Console.WriteLine("-- No full backups found.");
		return null;
	}

	// get most recent backup file less than or equal to the specified point in time
	var mostRecentBackup = fullbackups.Where(x => x.FileDate <= restorePointInTime).FirstOrDefault();
	if (mostRecentBackup == null)
	{
		Console.WriteLine($"-- No full backup found that matches the given point in time: {restorePointInTime.ToShortDateString()}");
		return null;
	}
	
	return mostRecentBackup;
}

public BackupFile GetMostRecentMatchingDiffBackup(string serverName, string dbName, string olaRootBackupFolder, DateTime restorePointInTime, DateTime mostRecentBackupDatetime, out DateTime tlogsAfter)
{
	var diffbackups = GetBackupFiles(serverName, dbName, olaRootBackupFolder, BackupType.DIFF);
	BackupFile mostRecentDiffBackup = null;

	if (diffbackups.Count > 0)
	{
		mostRecentDiffBackup = diffbackups.Where(x => x.FileDate <= restorePointInTime && x.FileDate >= mostRecentBackupDatetime).FirstOrDefault();
		tlogsAfter = mostRecentDiffBackup.FileDate;
	}
	else
	{
		tlogsAfter = mostRecentBackupDatetime;
	}
	
	return mostRecentDiffBackup;
}

public BackupFile GetMostRecentMatchingBackup(string serverName, string dbName, string olaRootBackupFolder, DateTime restorePointInTime)
{
	var fullbackups = GetBackupFiles(serverName, dbName, olaRootBackupFolder, BackupType.FULL);

	if (fullbackups.Count == 0)
	{
		Console.WriteLine("-- No full backups found.");
		return null;
	}

	// get most recent backup file less than or equal to the specified point in time
	var mostRecentBackup = fullbackups.Where(x => x.FileDate <= restorePointInTime).FirstOrDefault();
	if (mostRecentBackup == null)
	{
		Console.WriteLine($"-- No full backup found that matches the given point in time: {restorePointInTime.ToShortDateString()}");
		return null;
	}

	return mostRecentBackup;
}


public enum BackupType
{
	FULL = 1,
	DIFF = 2,
	TLOG = 3
}

public class BackupFile
{
	public string FilenameBase;
	public string FilenamePath;
	public DateTime FileDate;
	public int FileCount;
	public BackupType BackupType;

	public List<string> Files;

	public string GenerateRestoreStatement(string dbName, DateTime? stopAt)
	{
		string s = $"RESTORE DATABASE [{dbName}] FROM " + Environment.NewLine;
		
		for (int i = 0; i < this.FileCount; i++)
		{
			s += $"   DISK = N'{Path.Combine(this.FilenamePath, this.Files[i])}'{((i != this.FileCount - 1) ? "," : "")}" + Environment.NewLine;
		}
		s += " WITH NORECOVERY" + ((BackupType == BackupType.FULL) ? ", REPLACE" : "");

		if (stopAt.HasValue && BackupType == BackupType.TLOG)
		{
			s += ", STOPAT = '" + stopAt.Value.ToString("yyyy-MM-dd HH:mm:ss") + "'";
		}
		
		s += Environment.NewLine + Environment.NewLine;

		return s;
	}
}

public DateTime ConvertToDatetime(string date, string time)
{
	return DateTime.ParseExact(date + " " + time, "yyyyMMdd HHmmss", System.Globalization.CultureInfo.InvariantCulture);
}

public List<BackupFile> GetBackupFiles(string serverName, string dbName, string olaBackupFolder, BackupType backupType)
{
	List<BackupFile> result = new List<BackupFile>();
	Dictionary<string, BackupFile> dict = new Dictionary<string, BackupFile>();
	string basefilename;
	string extension;
	string folder;

	switch (backupType)
	{
		case BackupType.FULL:
			extension = ".bak";
			folder = "FULL";
			break;
			
		case BackupType.DIFF:
			extension = ".bak";
			folder = "DIFF";
			break;
			
		case BackupType.TLOG:
			extension = ".trn";
			folder = "LOG";
			break;

		default:
			throw new Exception();
	}

	string fullBackupPath = Path.Combine(olaBackupFolder, serverName, dbName, folder);
	string filePattern = serverName + "_" + dbName + "_" + $"{folder}_" + @"(\d{8})" + "_" + @"(\d{6})" + "([_0-9]*)" + extension;

	try
	{
		var fullBackupFiles = Directory.EnumerateFiles(fullBackupPath, "*" + extension, SearchOption.TopDirectoryOnly);

		foreach (string file in fullBackupFiles)
		{
			string filename = Path.GetFileName(file);

			Match m = Regex.Match(filename, filePattern, RegexOptions.IgnoreCase);

			if (!m.Success || m.Groups.Count < 3)
			{
				// mismatch on filename.....
				continue;
			}

			if (m.Groups.Count == 4 && !string.IsNullOrEmpty(m.Groups[3].ToString()))
			{
				// multi file backup set
				basefilename = filename.Substring(0, filename.Length - 4 - m.Groups[3].Length);
			}
			else
			{
				basefilename = filename;
			}

			if (!dict.ContainsKey(basefilename))
			{
				var f = new BackupFile()
				{
					FileCount = 1,
					FilenameBase = basefilename,
					FilenamePath = fullBackupPath,
					Files = new List<string>(),
					BackupType = backupType,
					FileDate = ConvertToDatetime(m.Groups[1].ToString(), m.Groups[2].ToString())
				};
				f.Files.Add(filename);

				result.Add(f);

				dict[basefilename] = f;
			}
			else
			{
				// seen filename base before (multi backup files in backup)
				var f = dict[basefilename];

				f.FileCount += 1;
				f.Files.Add(filename);
			}
		}
	}
	catch (Exception e)
	{
		Console.WriteLine(e.Message);
	}

	return result.OrderByDescending(o => o.FileDate).ToList();
}
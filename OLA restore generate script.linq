<Query Kind="Program" />

void Main()
{
	// Generate a database restore script from Ole Hallengren's Full backup solution.
	// https://ola.hallengren.com/
	//
	// Procedure:
	// 1. Find most recent FULL backup less than or equal to the specified date
	// 2. Look for any Differential backups after the FULL backup date; if found apply the latest DIFF backup less than or equal to the specified date
	// 3. Apply any TLOG backups up to the specified date 
	// 4. Take database out of recovery mode.
	
	var options = new Options();

	options.SourceServerName = "K7";
	options.SourceDbName = "AdventureWorks";
	options.OlaRootBackupFolder = @"C:\temp\Backup\";
	
	options.TargetDbName = @"Copy of AdventureWorks";
	options.TargetServerName = "K7";		
	options.TargetDbDataFilesLocation = @"C:\TempSqlDataFiles\DATA";  // null or empty means same folder structure as source
	options.TargetDbTLogFilesLocation = @"C:\TempSqlDataFiles\LOGS";  // null or empty means same folder structure as source

	// Point in time: (year, month, day, hours, minutes, seconds)
	//o.restorePointInTime = new DateTime(2021, 11, 18, 16, 15, 00);
	// ...or latest available
	options.RestorePointInTime = DateTime.Now;

	var s = GenerateRestoreScript(options);
	
	Console.WriteLine("USE [master]" + Environment.NewLine);	
	Console.WriteLine(s);
}

public class Options
{
	public string SourceServerName;
	public string SourceDbName;
	public string OlaRootBackupFolder;
	
	public string TargetServerName;	
	public string TargetDbName;
	public string TargetDbDataFilesLocation;
	public string TargetDbTLogFilesLocation;
	
	public DateTime RestorePointInTime;
} 

public string GenerateRestoreScript(Options o)
{
	var mostRecentBackup = GetMostRecentMatchingFullBackup(o);
	if (mostRecentBackup == null)
		return "";
	string cmd = mostRecentBackup.GenerateRestoreStatement(o, null);

	var mostRecentDiffBackup = GetMostRecentMatchingDiffBackup(o.SourceServerName, o.SourceDbName, o.OlaRootBackupFolder, o.RestorePointInTime, mostRecentBackup.FileDate, out DateTime tlogsAfter);
	if (mostRecentDiffBackup == null)
	{
		Console.WriteLine("-- No diff backup matches the given point in time, will attempt to generate restore TLog chain...");
	}
	else
	{
		cmd += mostRecentDiffBackup.GenerateRestoreStatement(o, o.RestorePointInTime);		
	}

	var tlogs = GetBackupFiles(o.SourceServerName, o.SourceDbName, o.OlaRootBackupFolder, BackupType.TLOG);
	var tlogsToApply = tlogs.Where(x => x.FileDate <= o.RestorePointInTime && x.FileDate >= tlogsAfter).ToList();
	foreach (var log in tlogsToApply)
	{
		cmd += log.GenerateRestoreStatement(o, o.RestorePointInTime);
	}

	cmd += $"RESTORE DATABASE [{o.TargetDbName}] WITH RECOVERY";
	
	return cmd;
}

public BackupFile GetMostRecentMatchingFullBackup(Options o) //(string sourceServerName, string sourceDbName, string olaRootBackupFolder, DateTime restorePointInTime)
{
	var fullbackups = GetBackupFiles(o.SourceServerName, o.SourceDbName, o.OlaRootBackupFolder, BackupType.FULL);
	
	if (fullbackups.Count == 0)
	{
		Console.WriteLine("-- No full backups found.");
		return null;
	}

	// get most recent backup file less than or equal to the specified point in time
	var mostRecentBackup = fullbackups.Where(x => x.FileDate <= o.RestorePointInTime).FirstOrDefault();
	if (mostRecentBackup == null)
	{
		Console.WriteLine($"-- No full backup found that matches the given point in time: {o.RestorePointInTime.ToShortDateString()}");
		return null;
	}

	if (!string.IsNullOrEmpty(o.TargetDbDataFilesLocation) && !string.IsNullOrEmpty(o.TargetDbTLogFilesLocation))
	{
	    mostRecentBackup.ReadDBFiles(o.TargetServerName);
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

	private List<DBFile> DBFiles;
	public List<string> Files;
	
	public string GenerateRestoreStatement(Options o, DateTime? stopAt)
	{
		string s = $"RESTORE DATABASE [{o.TargetDbName}] FROM " + Environment.NewLine;
		
		for (int i = 0; i < this.FileCount; i++)
		{
			s += $"   DISK = N'{Path.Combine(this.FilenamePath, this.Files[i])}'{((i != this.FileCount - 1) ? "," : "")}" + Environment.NewLine;
		}
		s += " WITH NORECOVERY" + ((BackupType == BackupType.FULL) ? ", REPLACE" : "") + Environment.NewLine;

		if (DBFiles != null)
		{
			string targetFolder;
			foreach (var f in DBFiles)
			{
				targetFolder = (f.Type == "L") ? o.TargetDbTLogFilesLocation : o.TargetDbDataFilesLocation;
				s += $"  , MOVE '{f.LogicalName}' TO '{Path.Combine(targetFolder, Path.GetFileName(f.PhysicalName))}'" + Environment.NewLine;
			}
		}

		if (stopAt.HasValue && BackupType == BackupType.TLOG)
		{
			s += ", STOPAT = '" + stopAt.Value.ToString("yyyy-MM-dd HH:mm:ss") + "'";
		}
		
		s += Environment.NewLine + Environment.NewLine;

		return s;
	}
	
	public void ReadDBFiles(string targetServername)
	{
		DBFiles = GetDBFileList(targetServername, Path.Combine(this.FilenamePath, this.Files[0]));
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

public static List<DBFile> GetDBFileList(string targetServer, string backupFileFullPath)
{
	DataTable dt = GetDBFiles(targetServer, backupFileFullPath);

	var results = dt.AsEnumerable()
					.Select(row =>
							new DBFile
							{
								LogicalName = row.Field<string>("LogicalName"),
								PhysicalName = row.Field<string>("PhysicalName"),
								Type = row.Field<string>("Type"),
								FileGroupName = row.Field<string>("FileGroupName"),
							})
					.ToList();
	return results;
}

public class DBFile
{
	public string LogicalName;
	public string PhysicalName;
	public string Type;
	public string FileGroupName;
}

private static DataTable GetDBFiles(string targetServer, string backupFileFullPath)
{
	var dt = new DataTable();

	string connectionstring = $"Server={targetServer};Database=master;Trusted_Connection=True;Persist Security Info=False;";

	using (var cn = new SqlConnection(connectionstring))
	using (SqlCommand cmd = cn.CreateCommand())
	{
		cmd.CommandType = CommandType.Text;
		cmd.CommandText = "RESTORE FILELISTONLY FROM DISK = @BackupFileFullPath;";

		SqlParameter backupfile = cmd.CreateParameter();
		backupfile.ParameterName = "@BackupFileFullPath";
		backupfile.DbType = DbType.String;
		backupfile.Value = backupFileFullPath;

		cmd.Parameters.Add(backupfile);

		cn.Open();

		using (IDataReader sqlDataReader = cmd.ExecuteReader())
		{
			dt.BeginLoadData();
			dt.Load(sqlDataReader);
			dt.EndLoadData();
			dt.AcceptChanges();
		}
	}

	return ConvertColumnDataTypes(dt);
}

private static DataTable ConvertColumnDataTypes(DataTable dt)
{
	bool requiresConvert = false;

	foreach (DataColumn col in dt.Columns)
	{
		if (col.DataType == typeof(uint))
		{
			requiresConvert = true;
			break;
		}
	}

	if (!requiresConvert)
		return dt;

	DataTable dtCloned = dt.Clone();

	// Change any Uint columns to int...
	foreach (DataColumn col in dtCloned.Columns)
	{
		if (col.DataType == typeof(uint))
		{
			col.DataType = typeof(int);
		}
	}

	foreach (DataRow row in dt.Rows)
	{
		dtCloned.ImportRow(row);
	}

	return dtCloned;
}
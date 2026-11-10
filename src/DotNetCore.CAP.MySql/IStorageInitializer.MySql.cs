// Copyright (c) .NET Core Community. All rights reserved.
// Licensed under the MIT License. See License.txt in the project root for license information.

using System;
using System.Threading;
using System.Threading.Tasks;
using DotNetCore.CAP.Persistence;
using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Options;
using MySqlConnector;

namespace DotNetCore.CAP.MySql;

public class MySqlStorageInitializer : IStorageInitializer
{
    private readonly IOptions<CapOptions> _capOptions;
    private readonly ILogger _logger;
    private readonly IOptions<MySqlOptions> _options;

    private ServerVersion? _serverVersion;

    public MySqlStorageInitializer(
        ILogger<MySqlStorageInitializer> logger,
        IOptions<MySqlOptions> options,
        IOptions<CapOptions> capOptions)
    {
        _options = options;
        _logger = logger;
        _capOptions = capOptions;
    }

    public virtual string GetPublishedTableName()
    {
        return $"{_options.Value.TableNamePrefix}.published";
    }

    public virtual string GetReceivedTableName()
    {
        return $"{_options.Value.TableNamePrefix}.received";
    }

    public virtual string GetLockTableName()
    {
        return $"{_options.Value.TableNamePrefix}.lock";
    }

    public async Task InitializeAsync(CancellationToken cancellationToken)
    {
        if (cancellationToken.IsCancellationRequested) return;

        var sql = CreateDbTablesScript();
        var connection = new MySqlConnection(_options.Value.ConnectionString);
        await using (connection.ConfigureAwait(false))
        {
            await connection.OpenAsync(cancellationToken);
            _serverVersion = ServerVersion.Parse(connection.ServerVersion);
            object[] sqlParams =
            {
                new MySqlParameter("@PubKey", $"publish_retry_{_capOptions.Value.Version}"),
                new MySqlParameter("@RecKey", $"received_retry_{_capOptions.Value.Version}"),
                new MySqlParameter("@LastLockTime", DateTime.MinValue)
            };
            await connection.ExecuteNonQueryAsync(sql, sqlParams: sqlParams).ConfigureAwait(false);
            await MigrateReceivedTableAsync(connection).ConfigureAwait(false);
        }

        _logger.LogDebug("Ensuring all create database tables script are applied.");
    }

    // MySQL has no ADD COLUMN / CREATE INDEX IF NOT EXISTS, so tables created by an older version are migrated here.
    // Instances starting together can both pass a guard; the loser's duplicate-column/key error is swallowed.
    private async Task MigrateReceivedTableAsync(MySqlConnection connection)
    {
        var table = new MySqlParameter("@Table", GetReceivedTableName());

        var hasColumn = await connection.ExecuteScalarAsync<long>(
            "SELECT COUNT(*) FROM information_schema.COLUMNS WHERE TABLE_SCHEMA=DATABASE() AND TABLE_NAME=@Table AND COLUMN_NAME='LeasedUntil';",
            table).ConfigureAwait(false);
        if (hasColumn == 0)
            await ExecuteIgnoringAsync(connection, $"ALTER TABLE `{GetReceivedTableName()}` ADD COLUMN `LeasedUntil` datetime DEFAULT NULL;", MySqlErrorCode.DuplicateFieldName).ConfigureAwait(false);

        var hasNewIndex = await connection.ExecuteScalarAsync<long>(
            "SELECT COUNT(DISTINCT INDEX_NAME) FROM information_schema.STATISTICS WHERE TABLE_SCHEMA=DATABASE() AND TABLE_NAME=@Table AND INDEX_NAME='IX_Version_StatusName_Added_LeasedUntil';",
            table).ConfigureAwait(false);
        if (hasNewIndex == 0)
            await ExecuteIgnoringAsync(connection, $"CREATE INDEX `IX_Version_StatusName_Added_LeasedUntil` ON `{GetReceivedTableName()}` (`Version`, `StatusName`, `Added`, `LeasedUntil`);", MySqlErrorCode.DuplicateKeyName).ConfigureAwait(false);

        var hasOldIndex = await connection.ExecuteScalarAsync<long>(
            "SELECT COUNT(DISTINCT INDEX_NAME) FROM information_schema.STATISTICS WHERE TABLE_SCHEMA=DATABASE() AND TABLE_NAME=@Table AND INDEX_NAME='IX_Version_ExpiresAt_StatusName';",
            table).ConfigureAwait(false);
        if (hasOldIndex > 0)
            await ExecuteIgnoringAsync(connection, $"DROP INDEX `IX_Version_ExpiresAt_StatusName` ON `{GetReceivedTableName()}`;", MySqlErrorCode.CannotDropFieldOrKey).ConfigureAwait(false);
    }

    private static async Task ExecuteIgnoringAsync(MySqlConnection connection, string sql, MySqlErrorCode ignored)
    {
        try
        {
            await connection.ExecuteNonQueryAsync(sql).ConfigureAwait(false);
        }
        catch (MySqlException ex) when (ex.ErrorCode == ignored)
        {
        }
    }

    public virtual bool IsSupportSkipLocked()
    {
        if (_serverVersion == null) return false;

        switch (_serverVersion.Type)
        {
            case ServerVersion.ServerType.MySql when _serverVersion.Version.Major >= 8:
            case ServerVersion.ServerType.MariaDb when _serverVersion.Version.Major > 10:
            case ServerVersion.ServerType.MariaDb
                when _serverVersion.Version.Major == 10 && _serverVersion.Version.Minor >= 6:
                return true;
            default:
                return false;
        }
    }


    protected virtual string CreateDbTablesScript()
    {
        var batchSql =
            $@"
CREATE TABLE IF NOT EXISTS `{GetReceivedTableName()}` (
  `Id` bigint NOT NULL,
  `Version` varchar(20) DEFAULT NULL,
  `Name` varchar(400) NOT NULL,
  `Group` varchar(200) DEFAULT NULL,
  `Content` longtext,
  `Retries` int(11) DEFAULT NULL,
  `Added` datetime NOT NULL,
  `ExpiresAt` datetime DEFAULT NULL,
  `StatusName` varchar(50) NOT NULL,
  `LeasedUntil` datetime DEFAULT NULL,
  PRIMARY KEY (`Id`),
  INDEX `IX_Version_StatusName_Added_LeasedUntil` (`Version`, `StatusName`, `Added`, `LeasedUntil`),
  INDEX `IX_ExpiresAt_StatusName` (`ExpiresAt`, `StatusName`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;

CREATE TABLE IF NOT EXISTS `{GetPublishedTableName()}` (
  `Id` bigint NOT NULL,
  `Version` varchar(20) DEFAULT NULL,
  `Name` varchar(200) NOT NULL,
  `Content` longtext,
  `Retries` int(11) DEFAULT NULL,
  `Added` datetime NOT NULL,
  `ExpiresAt` datetime DEFAULT NULL,
  `StatusName` varchar(40) NOT NULL,
  PRIMARY KEY (`Id`),
  INDEX `IX_Version_ExpiresAt_StatusName` (`Version`, `ExpiresAt`, `StatusName`),
  INDEX `IX_ExpiresAt_StatusName` (`ExpiresAt`, `StatusName`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;
";
        if (_capOptions.Value.UseStorageLock)
            batchSql += $@"
CREATE TABLE IF NOT EXISTS `{GetLockTableName()}` (
  `Key` varchar(128) NOT NULL,
  `Instance` varchar(256) DEFAULT NULL,
  `LastLockTime` datetime DEFAULT NULL,
  PRIMARY KEY (`Key`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;

INSERT IGNORE INTO `{GetLockTableName()}` (`Key`,`Instance`,`LastLockTime`) VALUES(@PubKey,'',@LastLockTime);
INSERT IGNORE INTO `{GetLockTableName()}` (`Key`,`Instance`,`LastLockTime`) VALUES(@RecKey,'',@LastLockTime);";

        return batchSql;
    }
}
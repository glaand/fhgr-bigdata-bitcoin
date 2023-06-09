--# Needed if you want to enable compression
--SET GLOBAL innodb_file_per_table=1;
--SET GLOBAL innodb_file_format=Barracuda;


SET default_storage_engine=INNODB;

CREATE SCHEMA IF NOT EXISTS btc_blockchain CHARACTER SET ascii COLLATE ascii_bin;
USE btc_blockchain;


DROP TABLE IF EXISTS blocks;
CREATE TABLE blocks (
  id              int check (id > 0) AUTO_INCREMENT      NOT NULL,
  hash 			binary(32)                          NOT NULL,
  height			int check (height > 0)					NOT NULL,
  version 		int                     		NOT NULL,
  blocksize		int check (blocksize > 0)					NOT NULL,
  hashPrev 		binary(32)                          NOT NULL,
  hashMerkleRoot 	binary(32)                          NOT NULL,
  nTime 			int check (nTime > 0)                    NOT NULL,
  nBits 			int check (nBits > 0)                    NOT NULL,
  nNonce 			int check (nNonce > 0)                    NOT NULL,

  PRIMARY KEY (id)
) ;
--  ROW_FORMAT=DYNAMIC;


DROP TABLE IF EXISTS transactions;
CREATE TABLE transactions (
  id              int check (id > 0) AUTO_INCREMENT      NOT NULL,
  txid            binary(32)                          NOT NULL,
  hashBlock       binary(32)                          NOT NULL,
  version         int check (version > 0)               		NOT NULL,
  lockTime        int check (lockTime > 0)     				NOT NULL,

  PRIMARY KEY (id)
) ;
--  ROW_FORMAT=DYNAMIC;


DROP TABLE IF EXISTS tx_out;
CREATE TABLE tx_out (
  id              int check (id > 0) AUTO_INCREMENT      NOT NULL,
  txid            binary(32)                  		NOT NULL,
  indexOut        int check (indexOut > 0)            		NOT NULL,
  value           bigint check (value > 0)            		NOT NULL,
  scriptPubKey    blob                                NOT NULL,
  address     	varchar(36) 					DEFAULT NULL,
  unspent        	bit DEFAULT TRUE                    NOT NULL,

  PRIMARY KEY (id)
) ;
--  ROW_FORMAT=DYNAMIC;


DROP TABLE IF EXISTS tx_in;
CREATE TABLE tx_in (
  id              int check (id > 0) AUTO_INCREMENT     NOT NULL,
  txid            binary(32)                          NOT NULL,
  hashPrevOut     binary(32)                          NOT NULL,
  indexPrevOut    int check (indexPrevOut > 0)                    NOT NULL,
  scriptSig       blob                                NOT NULL,
  sequence        int check (sequence > 0)                    NOT NULL,

  PRIMARY KEY (id)
) ;
--  ROW_FORMAT=DYNAMIC;

SET @@session.unique_checks = 0;
SET @@session.foreign_key_checks = 0;
SET @@session.sync_binlog = 0;

-- Speed up bulk load
SET autocommit = 0;
SET sql_safe_updates = 0;
SET sql_log_bin=0;


--# Also consider to set following mysql settings for maximum performance
-- innodb_read_io_threads 	= 3
-- innodb_write_io_threads 	= 3
-- innodb_buffer_pool_size 	= 6G
-- innodb_autoinc_lock_mode 	= 2
-- innodb_log_file_size 		= 128M
-- innodb_log_buffer_size 	= 8M
-- innodb_flush_method 		= O_DIRECT
-- innodb_flush_log_at_trx_commit = 0
-- skip-innodb_doublewrite


TRUNCATE blocks;
--# Load blocks into table
LOAD DATA INFILE '/media/tmp/dump/blocks-0-393489.csv'
INTO TABLE blocks
FIELDS TERMINATED BY ';'
LINES TERMINATED BY 'n'
(@hash, height, version, blocksize, @hashPrev, @hashMerkleRoot, nTime, nBits, nNonce)
SET hash = unhex(@hash),
	hashPrev = unhex(@hashPrev),
    hashMerkleRoot = unhex(@hashMerkleRoot);
COMMIT;


TRUNCATE transactions;
--# Load transactions into table
LOAD DATA INFILE '/media/tmp/dump/transactions-0-393489.csv'
INTO TABLE transactions
FIELDS TERMINATED BY ';'
LINES TERMINATED BY 'n'
(@txid, @hashBlock, version, lockTime)
SET txid = unhex(@txid),
	hashBlock = unhex(@hashBlock);
COMMIT;


TRUNCATE tx_out;
--# Load tx_out into table
LOAD DATA INFILE '/media/tmp/dump/tx_out-0-393489.csv'
INTO TABLE tx_out
FIELDS TERMINATED BY ';'
LINES TERMINATED BY 'n'
(@txid, indexOut, value, @scriptPubKey, address)
SET txid = unhex(@txid),
	scriptPubKey = unhex(@scriptPubKey);
COMMIT;


TRUNCATE tx_in;
--# Load tx_in into table
LOAD DATA INFILE '/media/tmp/dump/tx_in-0-393489.csv'
INTO TABLE tx_in
FIELDS TERMINATED BY ';'
LINES TERMINATED BY 'n'
(@txid, @hashPrevOut, indexPrevOut, scriptSig, sequence)
SET txid = unhex(@txid),
	hashPrevOut = unhex(@hashPrevOut);
COMMIT;


SET @@session.unique_checks = 1;
SET @@session.foreign_key_checks = 1;

-- Add keys
ALTER TABLE blocks ADD UNIQUE (hash);
ALTER TABLE transactions ADD CREATE INDEX (txid);
ALTER TABLE tx_in ADD CREATE INDEX (hashPrevOut, indexPrevOut);
ALTER TABLE tx_out ADD CREATE INDEX (txid, indexOut),
					 ADD KEY (address);
--ALTER TABLE `transactions` ADD FOREIGN KEY (`hashBlock`) REFERENCES blocks(`hash`);
--ALTER TABLE `tx_out` ADD FOREIGN KEY (`txid`) REFERENCES transactions(`txid`);
--ALTER TABLE `tx_id` ADD FOREIGN KEY (`txid`) REFERENCES transactions(`txid`);
COMMIT;

-- Flag spent tx outputs
UPDATE tx_out o, tx_in i
	SET o.unspent = FALSE
WHERE o.txid = i.hashPrevOut
  AND o.indexOut = i.indexPrevOut;
COMMIT;


SET autocommit = 1;
SET sql_log_bin=1;
SET @@session.sync_binlog=1;
SET sql_safe_updates = 1;
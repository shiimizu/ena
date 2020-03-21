//! MySQL implementation.

#![allow(dead_code)]
#![allow(unused_variables)]
#![allow(unused_imports)]
use crate::{
    archiver::YotsubaArchiver,
    enums::{YotsubaBoard, YotsubaEndpoint, YotsubaHash, YotsubaIdentifier},
    sql::*
};
use anyhow::{anyhow, Result};
use async_trait::async_trait;
use mysql_async::{prelude::*, Pool, Row};
use std::{
    boxed::Box,
    collections::VecDeque,
    convert::TryFrom,
    sync::{Arc, Mutex}
};

pub type Statement = mysql_async::Stmt<mysql_async::Conn>;

#[cold]
#[allow(dead_code)]
pub mod asagi {

    #[cold]
    #[allow(dead_code)]
    pub struct Post {
        pub poster_ip:            i32,
        pub num:                  i32,
        pub subnum:               i32,
        pub thread_num:           i32,
        pub unique_ips:           i32,
        pub since4pass:           i32,
        pub op:                   bool,
        pub date:                 i64,
        pub preview_orig:         String,
        pub preview_w:            i32,
        pub preview_h:            i32,
        pub media_id:             i32,
        pub media_orig:           String,
        pub media_w:              i32,
        pub media_h:              i32,
        pub media_size:           i32,
        pub media_hash:           String,
        pub media_filename:       String,
        pub spoiler:              bool,
        pub deleted:              bool,
        pub capcode:              String,
        pub email:                String,
        pub name:                 String,
        pub trip:                 String,
        pub title:                String,
        pub comment:              String,
        pub delpass:              String,
        pub sticky:               bool,
        pub closed:               bool,
        pub archived:             bool,
        pub poster_hash:          String,
        pub poster_country:       String,
        pub poster_troll_country: String,
        pub exif:                 String,
        pub link:                 String,
        pub r#type:               String,
        pub omitted:              bool
    }
}

#[async_trait]
impl Archiver for YotsubaArchiver<Statement, mysql_async::Row, Pool, reqwest::Client> {
    async fn run_inner(&self) -> Result<()> {
        Ok(self.run().await?)
    }
}

impl Queries for Pool {
    // MySQL concurrency and lock [issues](https://dba.stackexchange.com/a/206279)
    fn query_init_schema(&self, schema: &str, engine: Database, charset: &str) -> String {
        // init commons.sql and functions
        format!(
            r#"
        SET SESSION transaction_isolation='READ-COMMITTED';
        begin;
        -- SET GLOBAL binlog_format = 'ROW';
        
        CREATE TABLE IF NOT EXISTS `index_counters` (
            `id` varchar(50) NOT NULL,
            `val` int(10) NOT NULL,
            PRIMARY KEY (`id`)
          ) ENGINE={engine} DEFAULT CHARSET={charset};
          
          
          DROP FUNCTION IF EXISTS doCleanFull;
          CREATE FUNCTION doCleanFull (com TEXT)
          RETURNS TEXT DETERMINISTIC
          RETURN	
              REGEXP_REPLACE(
              REGEXP_REPLACE(
              REGEXP_REPLACE(
              REGEXP_REPLACE(
              REGEXP_REPLACE(
              REGEXP_REPLACE(
              REGEXP_REPLACE(
              REGEXP_REPLACE(
              REGEXP_REPLACE(
              REGEXP_REPLACE(
              REGEXP_REPLACE(
              REGEXP_REPLACE(
              REPLACE(
              REGEXP_REPLACE(
              REGEXP_REPLACE(
              REGEXP_REPLACE(
              REGEXP_REPLACE(
              REGEXP_REPLACE(
              REGEXP_REPLACE(
              REGEXP_REPLACE(
              REGEXP_REPLACE(
              REGEXP_REPLACE(
              REGEXP_REPLACE(
              REGEXP_REPLACE(
              REGEXP_REPLACE(
              REGEXP_REPLACE(
              REGEXP_REPLACE(	
              REGEXP_REPLACE(
              REGEXP_REPLACE(
              REGEXP_REPLACE(com, '&#039;', '\'')
              , '&gt;', '>')
              , '&lt;', '<')
              , '&quot;', '"')
              , '&amp;', '&')
              , '\\s*$', '')
              , '^\\s*$', '')
              , '<span class=\"capcodeReplies\"><span style=\"font-size: smaller;\"><span style=\"font-weight: bold;\">(?:Administrator|Moderator|Developer) Repl(?:y|ies):</span>.*?</span><br></span>', '')
              , '\\[(/?(banned|moot|spoiler|code))]', '[$1:lit]')
              , '<span class=\"abbr\">.*?</span>', '')
              , '<table class=\"exif\"[^>]*>.*?</table>', '')
              , '<br><br><small><b>Oekaki Post</b>.*?</small>', '')
              , '<(?:b|strong) style=\"color:\\s*red;\">(.*?)</(?:b|strong)>', '[banned]$1[/banned]')
              , '<div style=\"padding: 5px;margin-left: \\.5em;border-color: #faa;border: 2px dashed rgba\\(255,0,0,\\.1\\);border-radius: 2px\">(.*?)</div>', '[moot]$1[/moot]')
              , '<span class=\"fortune\" style=\"color:(.*?)\"><br><br><b>(.*?)</b></span>', '\n\n[fortune color=\"$1\"]$2[/fortune]')
              , '<(?:b|strong)>(.*?)</(?:b|strong)>', '[b]$1[/b]')
              , '<pre[^>]*>', '[code]')
              , '</pre>', '[/code]')
              , '<span class=\"math\">(.*?)</span>', '[math]$1[/math]')
              , '<div class=\"math\">(.*?)</div>', '[eqn]$1[/eqn]')
              , '<font class=\"unkfunc\">(.*?)</font>', '$1')
              , '<span class=\"quote\">(.*?)</span>', '$1')
              , '<span class=\"(?:[^\"]*)?deadlink\">(.*?)</span>', '$1')
              , '<a[^>]*>(.*?)</a>', '$1')
              , '<span class=\"spoiler\"[^>]*>(.*?)</span>', '[spoiler]$1[/spoiler]')
              , '<span class=\"sjis\">(.*?)</span>', '[shiftjis]$1[/shiftjis]')
              , '<s>', '[spoiler]')
              , '</s>', '[/spoiler]')
              , '<wbr>', '')
              , '<br\\s*/?>', '\n');
          "#,
            engine = engine.mysql_engine(),
            charset = charset
        )
    }

    fn query_init_metadata(&self, engine: Database, charset: &str) -> String {
        format!(
            "
            CREATE TABLE IF NOT EXISTS `metadata` (
                `board` varchar(10) NOT NULL PRIMARY key unique ,
                `threads` json,
                `archive` json,
                INDEX metadata_board_idx (`board`)
            ) ENGINE={engine} CHARSET={charset} COLLATE={charset}_general_ci;
            ",
            engine = engine.mysql_engine(),
            charset = charset
        )
    }

    fn query_delete(&self, board: YotsubaBoard) -> String {
        format!(
            "UPDATE `{}` SET deleted = 1, timestamp_expired = unix_timestamp() WHERE num = ? AND subnum = 0",
            board
        )
    }

    // This query is probably inefficient because we query the same things twice
    fn query_update_deleteds(&self, board: YotsubaBoard) -> String {
        // This simulates a FULL JOIN
        format!(
            r#"
                UPDATE `{board}`, (
                    SELECT x.* FROM
                        (SELECT num, `timestamp`, thread_num FROM `{board}` where num=:no or thread_num=:no order by num) x
                    LEFT OUTER JOIN
                        ( {schema_4chan_query} ) z
                      ON x.num = z.no
                      UNION
                      
                    SELECT x.* FROM
                        (SELECT num, `timestamp`, thread_num FROM `{board}` where num=:no or thread_num=:no order by num) x
                    RIGHT OUTER JOIN
                        ( {schema_4chan_query} ) z
                      ON x.num = z.no
                    WHERE z.no is null
                ) as `src`
                SET `{board}`.deleted = 1;"#,
            board = board,
            schema_4chan_query = query_4chan_schema()
        )
    }

    fn query_update_hash(
        &self, board: YotsubaBoard, hash_type: YotsubaHash, thumb: YotsubaStatement
    ) -> String {
        // Do nothing
        "select 1".into()
    }

    fn query_update_metadata(&self, endpoint: YotsubaEndpoint) -> String {
        format!(
            r#"
            INSERT INTO `metadata`(`board`, `{endpoint}`)       
			SELECT :bb, (CASE WHEN (
            SELECT JSON_ARRAYAGG(`no`) from
                (SELECT * FROM metadata,JSON_TABLE(:jj, "{path1}" COLUMNS(
	            `no`				bigint		PATH "{path2}")
	            ) w )z 
            WHERE `no` IS NOT NULL
            ) IS NOT NULL
            THEN :jj ELSE 'panic!' END) as `check`
            
            ON DUPLICATE KEY update
               `{endpoint}` = ( select (CASE WHEN (
				            SELECT JSON_ARRAYAGG(`no`) from
				                (SELECT * FROM metadata,JSON_TABLE(:jj, "{path1}" COLUMNS(
					            `no`				bigint		PATH "{path2}")
					            ) w )z 
				            WHERE `no` IS NOT NULL
				            ) IS NOT NULL
				            THEN :jj ELSE 'panic!' END));"#,
            endpoint = endpoint,
            path1 = if endpoint == YotsubaEndpoint::Threads { "$[*].threads[*]" } else { "$[*]" },
            path2 = if endpoint == YotsubaEndpoint::Threads { "$.no" } else { "$" }
        )
    }

    fn query_medias(&self, board: YotsubaBoard, thumb: YotsubaStatement) -> String {
        format!(
            "SELECT * FROM `{0}`
                WHERE (media_hash is not null) AND (num=:no or thread_num=:no)
                ORDER BY num desc LOCK IN SHARE MODE;",
            board
        )
    }

    fn query_threads_modified(&self, endpoint: YotsubaEndpoint) -> String {
        match endpoint {
            YotsubaEndpoint::Threads => format!(
                r#"
            SELECT JSON_ARRAYAGG(coalesce (newv->'$.no', prev->'$.no')) from (
                SELECT * from metadata m2 ,
                    JSON_TABLE(threads, '$[*].threads[*]'
                    COLUMNS(prev json path '$')) q 
                LEFT OUTER JOIN
                (SELECT * from 
                    JSON_TABLE(:jj, '$[*].threads[*]'
                    COLUMNS(newv json path '$')) w) e
                on prev->'$.no' = newv->'$.no'
                where board = :bb
                
                UNION
                
                SELECT * from metadata m3 ,
                    JSON_TABLE( threads, '$[*].threads[*]'
                    COLUMNS(prev json path '$')) r
                RIGHT OUTER JOIN
                (SELECT * FROM
                    JSON_TABLE(:jj, '$[*].threads[*]'
                    COLUMNS(newv json path '$')) a) s
                on prev->'$.no' = newv->'$.no'
                where board = :bb
            ) z
            where newv is null or prev is null or prev->'$.last_modified' != newv->'$.last_modified' LOCK IN SHARE MODE;
            "#
            ),
            _ => format!(
                r#"
            SELECT JSON_ARRAYAGG(coalesce (newv,prev)) from (
                SELECT * from metadata m2 ,
                    JSON_TABLE( archive, '$[*]'
                    COLUMNS(prev json path '$')) q 
                LEFT OUTER JOIN
                (SELECT * from 
                    JSON_TABLE(:jj, '$[*]'
                    COLUMNS(newv json path '$')) w) e
                on prev = newv
                where board = :bb
                
                UNION
                
                SELECT * from metadata m3 ,
                    JSON_TABLE( archive, '$[*]'
                    COLUMNS(prev json path '$')) r
                RIGHT OUTER JOIN
                (SELECT * FROM
                    JSON_TABLE(:jj, '$[*]'
                    COLUMNS(newv json path '$')) a) s
                on prev = newv
                where board = :bb
            ) z
            where newv is null or prev is null;
            "#
            )
        }
    }

    fn query_threads(&self) -> String {
        r#"
            SELECT JSON_ARRAYAGG(no)
            FROM
            ( SELECT * FROM JSON_TABLE(:jj, "$[*].threads[*]" COLUMNS(
            `no`				bigint		PATH "$.no")
            ) w )z
            WHERE no is not null LOCK IN SHARE MODE;
        "#
        .to_string()
    }

    fn query_metadata(&self, endpoint: YotsubaEndpoint) -> String {
        // Endpoint will always be threads
        format!(
            r#"
            SELECT (CASE WHEN (
                SELECT JSON_ARRAYAGG(`no`) from
	                (SELECT * FROM metadata,JSON_TABLE(`{endpoint}`, "{path1}" COLUMNS(
		            `no`				bigint		PATH "{path2}")
		            ) w where board = :bb)z 
                WHERE `no` IS NOT NULL
                ) IS NOT NULL AND `{endpoint}` IS NOT NULL
                THEN true ELSE false END) as `check`
            FROM `metadata` WHERE board = :bb LOCK IN SHARE MODE;
            "#,
            endpoint = endpoint,
            path1 = if endpoint == YotsubaEndpoint::Threads { "$[*].threads[*]" } else { "$[*]" },
            path2 = if endpoint == YotsubaEndpoint::Threads { "$.no" } else { "$" }
        )
    }

    fn query_threads_combined(&self, board: YotsubaBoard, endpoint: YotsubaEndpoint) -> String {
        let thread = format!(
            r#"
        select JSON_ARRAYAGG(c) from (
            SELECT coalesce (newv->'$.no', prev->'$.no') as c from (
                SELECT * from metadata m2 ,
                    JSON_TABLE(threads, '$[*].threads[*]'
                    COLUMNS(prev json path '$')) q 
                LEFT OUTER JOIN
                (SELECT * from 
                    JSON_TABLE(:jj, '$[*].threads[*]'
                    COLUMNS(newv json path '$')) w) e
                on prev->'$.no' = newv->'$.no'
                where board = :bb
                
                UNION
                
                SELECT * from metadata m3 ,
                    JSON_TABLE( threads, '$[*].threads[*]'
                    COLUMNS(prev json path '$')) r
                RIGHT OUTER JOIN
                (SELECT * FROM
                    JSON_TABLE(:jj, '$[*].threads[*]'
                    COLUMNS(newv json path '$')) a) s
                on prev->'$.no' = newv->'$.no'
                where board = :bb
            ) z ) i
            left join
              (select num as nno from `{}` where op=1 and (timestamp_expired is not null or deleted is not null))u
              ON c = nno
            where  nno is null LOCK IN SHARE MODE;
        "#,
            board
        );

        let archive = format!(
            r#"
        select JSON_ARRAYAGG(c) from (
            SELECT coalesce (newv, prev) as c from (
                SELECT * from metadata m2 ,
                    JSON_TABLE(archive, '$[*]'
                    COLUMNS(prev json path '$')) q 
                LEFT OUTER JOIN
                (SELECT * from 
                    JSON_TABLE(:jj, '$[*]'
                    COLUMNS(newv json path '$')) w) e
                on prev = newv
                where board = :bb
                
                UNION
                
                SELECT * from metadata m3 ,
                    JSON_TABLE(archive, '$[*]'
                    COLUMNS(prev json path '$')) r
                RIGHT OUTER JOIN
                (SELECT * FROM
                    JSON_TABLE(:jj, '$[*]'
                    COLUMNS(newv json path '$')) a) s
                on prev = newv
                where board = :bb
            ) z ) i
            left join
              (select num as nno from `{}` where op=1 and (timestamp_expired is not null or deleted is not null))u
              ON c = nno
            where  nno is null LOCK IN SHARE MODE;
        "#,
            board
        );
        match endpoint {
            YotsubaEndpoint::Archive => archive,
            _ => thread
        }
    }

    // TODO speedup
    // https://www.mysqltutorial.org/mysql-stored-procedure/mysql-show-function/
    // https://www.mysqltutorial.org/listing-stored-procedures-in-mysql-database.aspx
    // https://www.mysqltutorial.org/mysql-exists/
    fn query_init_board(&self, board: YotsubaBoard, engine: Database, charset: &str) -> String {
        // Init boards and triggers
        format!(
            r#"CREATE TABLE IF NOT EXISTS `{board}` (
            `doc_id` int unsigned NOT NULL auto_increment,
            `media_id` int unsigned NOT NULL DEFAULT '0',
            `poster_ip` decimal(39,0) unsigned NOT NULL DEFAULT '0',
            `num` int unsigned NOT NULL,
            `subnum` int unsigned NOT NULL,
            `thread_num` int unsigned NOT NULL DEFAULT '0',
            `op` bool NOT NULL DEFAULT '0',
            `timestamp` int unsigned NOT NULL,
            `timestamp_expired` int unsigned NOT NULL,
            `preview_orig` varchar(20),
            `preview_w` smallint unsigned NOT NULL DEFAULT '0',
            `preview_h` smallint unsigned NOT NULL DEFAULT '0',
            `media_filename` text,
            `media_w` smallint unsigned NOT NULL DEFAULT '0',
            `media_h` smallint unsigned NOT NULL DEFAULT '0',
            `media_size` int unsigned NOT NULL DEFAULT '0',
            `media_hash` varchar(25),
            `media_orig` varchar(20),
            `spoiler` bool NOT NULL DEFAULT '0',
            `deleted` bool NOT NULL DEFAULT '0',
            `capcode` varchar(1) NOT NULL DEFAULT 'N',
            `email` varchar(100),
            `name` varchar(100),
            `trip` varchar(25),
            `title` varchar(100),
            `comment` text,
            `delpass` tinytext,
            `sticky` bool NOT NULL DEFAULT '0',
            `locked` bool NOT NULL DEFAULT '0',
            `poster_hash` varchar(8),
            `poster_country` varchar(2),
            `exif` text,
          
            PRIMARY KEY (`doc_id`),
            UNIQUE num_subnum_index (`num`, `subnum`),
            INDEX thread_num_subnum_index (`thread_num`, `num`, `subnum`),
            INDEX subnum_index (`subnum`),
            INDEX op_index (`op`),
            INDEX media_id_index (`media_id`),
            INDEX media_hash_index (`media_hash`),
            INDEX media_orig_index (`media_orig`),
            INDEX name_trip_index (`name`, `trip`),
            INDEX trip_index (`trip`),
            INDEX email_index (`email`),
            INDEX poster_ip_index (`poster_ip`),
            INDEX timestamp_index (`timestamp`)
          ) engine={engine} CHARSET={charset} COLLATE={charset}_general_ci;
          
          CREATE TABLE IF NOT EXISTS `{board}_deleted` LIKE `{board}`;


          CREATE TABLE IF NOT EXISTS `{board}_threads` (
            `thread_num` int unsigned NOT NULL,
            `time_op` int unsigned NOT NULL,
            `time_last` int unsigned NOT NULL,
            `time_bump` int unsigned NOT NULL,
            `time_ghost` int unsigned DEFAULT NULL,
            `time_ghost_bump` int unsigned DEFAULT NULL,
            `time_last_modified` int unsigned NOT NULL,
            `nreplies` int unsigned NOT NULL DEFAULT '0',
            `nimages` int unsigned NOT NULL DEFAULT '0',
            `sticky` bool NOT NULL DEFAULT '0',
            `locked` bool NOT NULL DEFAULT '0',
          
            PRIMARY KEY (`thread_num`),
            INDEX time_op_index (`time_op`),
            INDEX time_bump_index (`time_bump`),
            INDEX time_ghost_bump_index (`time_ghost_bump`),
            INDEX time_last_modified_index (`time_last_modified`),
            INDEX sticky_index (`sticky`),
            INDEX locked_index (`locked`)
          ) ENGINE={engine} CHARSET={charset} COLLATE={charset}_general_ci;
          
          
          CREATE TABLE IF NOT EXISTS `{board}_users` (
            `user_id` int unsigned NOT NULL auto_increment,
            `name` varchar(100) NOT NULL DEFAULT '',
            `trip` varchar(25) NOT NULL DEFAULT '',
            `firstseen` int(11) NOT NULL,
            `postcount` int(11) NOT NULL,
          
            PRIMARY KEY (`user_id`),
            UNIQUE name_trip_index (`name`, `trip`),
            INDEX firstseen_index (`firstseen`),
            INDEX postcount_index (`postcount`)
          ) ENGINE={engine} DEFAULT CHARSET={charset} COLLATE={charset}_general_ci;
          
          
          CREATE TABLE IF NOT EXISTS `{board}_images` (
            `media_id` int unsigned NOT NULL auto_increment,
            `media_hash` varchar(25) NOT NULL,
            `media` varchar(20),
            `preview_op` varchar(20),
            `preview_reply` varchar(20),
            `total` int(10) unsigned NOT NULL DEFAULT '0',
            `banned` smallint unsigned NOT NULL DEFAULT '0',
          
            PRIMARY KEY (`media_id`),
            UNIQUE media_hash_index (`media_hash`),
            INDEX total_index (`total`),
            INDEX banned_index (`banned`)
          ) ENGINE={engine} DEFAULT CHARSET={charset} COLLATE={charset}_general_ci;
          
          
          CREATE TABLE IF NOT EXISTS `{board}_daily` (
            `day` int(10) unsigned NOT NULL,
            `posts` int(10) unsigned NOT NULL,
            `images` int(10) unsigned NOT NULL,
            `sage` int(10) unsigned NOT NULL,
            `anons` int(10) unsigned NOT NULL,
            `trips` int(10) unsigned NOT NULL,
            `names` int(10) unsigned NOT NULL,
          
            PRIMARY KEY (`day`)
          ) ENGINE={engine} DEFAULT CHARSET={charset} COLLATE={charset}_general_ci;
          
          DROP PROCEDURE IF EXISTS `update_thread_{board}`;

          CREATE PROCEDURE `update_thread_{board}` (tnum INT, ghost_num INT, p_timestamp INT,
            p_media_hash VARCHAR(25), p_email VARCHAR(100))
          BEGIN
            DECLARE d_time_last INT;
            DECLARE d_time_bump INT;
            DECLARE d_time_ghost INT;
            DECLARE d_time_ghost_bump INT;
            DECLARE d_time_last_modified INT;
            DECLARE d_image INT;
          
            SET d_time_last = 0;
            SET d_time_bump = 0;
            SET d_time_ghost = 0;
            SET d_time_ghost_bump = 0;
            SET d_image = p_media_hash IS NOT NULL;
          
            IF (ghost_num = 0) THEN
              SET d_time_last_modified = p_timestamp;
              SET d_time_last = p_timestamp;
              IF (p_email <> 'sage' OR p_email IS NULL) THEN
                SET d_time_bump = p_timestamp;
              END IF;
            ELSE
              SET d_time_last_modified = p_timestamp;
              SET d_time_ghost = p_timestamp;
              IF (p_email <> 'sage' OR p_email IS NULL) THEN
                SET d_time_ghost_bump = p_timestamp;
              END IF;
            END IF;
          
            UPDATE
              `{board}_threads` op
            SET
              op.time_last = (
                COALESCE(
                  GREATEST(op.time_op, d_time_last),
                  op.time_op
                )
              ),
              op.time_bump = (
                COALESCE(
                  GREATEST(op.time_bump, d_time_bump),
                  op.time_op
                )
              ),
              op.time_ghost = (
                IF (
                  GREATEST(
                    IFNULL(op.time_ghost, 0),
                    d_time_ghost
                  ) <> 0,
                  GREATEST(
                    IFNULL(op.time_ghost, 0),
                    d_time_ghost
                  ),
                  NULL
                )
              ),
              op.time_ghost_bump = (
                IF(
                  GREATEST(
                    IFNULL(op.time_ghost_bump, 0),
                    d_time_ghost_bump
                  ) <> 0,
                  GREATEST(
                    IFNULL(op.time_ghost_bump, 0),
                    d_time_ghost_bump
                  ),
                  NULL
                )
              ),
              op.time_last_modified = (
                COALESCE(
                  GREATEST(op.time_last_modified, d_time_last_modified),
                  op.time_op
                )
              ),
              op.nreplies = (
                op.nreplies + 1
              ),
              op.nimages = (
                op.nimages + d_image
              )
              WHERE op.thread_num = tnum;
          END;
          
          DROP PROCEDURE IF EXISTS `update_thread_timestamp_{board}`;
          
          CREATE PROCEDURE `update_thread_timestamp_{board}` (tnum INT, timestamp INT)
          BEGIN
            UPDATE
              `{board}_threads` op
            SET
              op.time_last_modified = (
                GREATEST(op.time_last_modified, timestamp)
              )
            WHERE op.thread_num = tnum;
          END;
          
          DROP PROCEDURE IF EXISTS `create_thread_{board}`;
          
          CREATE PROCEDURE `create_thread_{board}` (num INT, timestamp INT)
          BEGIN
            INSERT IGNORE INTO `{board}_threads` VALUES (num, timestamp, timestamp,
              timestamp, NULL, NULL, timestamp, 0, 0, 0, 0);
          END;
          
          DROP PROCEDURE IF EXISTS `delete_thread_{board}`;
          
          CREATE PROCEDURE `delete_thread_{board}` (tnum INT)
          BEGIN
            DELETE FROM `{board}_threads` WHERE thread_num = tnum;
          END;
          
          DROP PROCEDURE IF EXISTS `insert_image_{board}`;
          
          CREATE PROCEDURE `insert_image_{board}` (n_media_hash VARCHAR(25),
           n_media VARCHAR(20), n_preview VARCHAR(20), n_op INT)
          BEGIN
            IF n_op = 1 THEN
              INSERT INTO `{board}_images` (media_hash, media, preview_op, total)
              VALUES (n_media_hash, n_media, n_preview, 1)
              ON DUPLICATE KEY UPDATE
                media_id = LAST_INSERT_ID(media_id),
                total = (total + 1),
                preview_op = COALESCE(preview_op, VALUES(preview_op)),
                media = COALESCE(media, VALUES(media));
            ELSE
              INSERT INTO `{board}_images` (media_hash, media, preview_reply, total)
              VALUES (n_media_hash, n_media, n_preview, 1)
              ON DUPLICATE KEY UPDATE
                media_id = LAST_INSERT_ID(media_id),
                total = (total + 1),
                preview_reply = COALESCE(preview_reply, VALUES(preview_reply)),
                media = COALESCE(media, VALUES(media));
            END IF;
          END;
          
          DROP PROCEDURE IF EXISTS `delete_image_{board}`;
          
          CREATE PROCEDURE `delete_image_{board}` (n_media_id INT)
          BEGIN
            UPDATE `{board}_images` SET total = (total - 1) WHERE media_id = n_media_id;
          END;
          
          DROP TRIGGER IF EXISTS `before_ins_{board}`;
          
          CREATE TRIGGER `before_ins_{board}` BEFORE INSERT ON `{board}`
          FOR EACH ROW
          BEGIN
            IF NEW.media_hash IS NOT NULL THEN
              CALL insert_image_{board}(NEW.media_hash, NEW.media_orig, NEW.preview_orig, NEW.op);
              SET NEW.media_id = LAST_INSERT_ID();
            END IF;
          END;
          
          DROP TRIGGER IF EXISTS `after_ins_{board}`;
          
          CREATE TRIGGER `after_ins_{board}` AFTER INSERT ON `{board}`
          FOR EACH ROW
          BEGIN
            IF NEW.op = 1 THEN
              CALL create_thread_{board}(NEW.num, NEW.timestamp);
            END IF;
            CALL update_thread_{board}(NEW.thread_num, NEW.subnum, NEW.timestamp, NEW.media_hash, NEW.email);
          END;
          
          DROP TRIGGER IF EXISTS `after_del_{board}`;
          
          CREATE TRIGGER `after_del_{board}` AFTER DELETE ON `{board}`
          FOR EACH ROW
          BEGIN
            CALL update_thread_{board}(OLD.thread_num, OLD.subnum, OLD.timestamp, OLD.media_hash, OLD.email);
            IF OLD.op = 1 THEN
              CALL delete_thread_{board}(OLD.num);
            END IF;
            IF OLD.media_hash IS NOT NULL THEN
              CALL delete_image_{board}(OLD.media_id);
            END IF;
          END;
          
          DROP TRIGGER IF EXISTS `after_upd_{board}`;
          
          CREATE TRIGGER `after_upd_{board}` AFTER UPDATE ON `{board}`
          FOR EACH ROW
          BEGIN
            IF NEW.timestamp_expired <> 0 THEN
              CALL update_thread_timestamp_{board}(NEW.thread_num, NEW.timestamp_expired);
            END IF;
          END;
          
          "#,
            board = board,
            engine = engine.mysql_engine(),
            charset = charset
        )
    }

    fn query_init_type(&self) -> String {
        // Do nothing
        "select 1".into()
    }

    fn query_init_views(&self, board: YotsubaBoard) -> String {
        // Do nothing
        "select 1".into()
    }

    fn query_update_thread(&self, board: YotsubaBoard) -> String {
        format!(
            r#"
        INSERT INTO `{}`(`poster_ip`,`num`,`subnum`,`thread_num`,`op`,`timestamp`,`timestamp_expired`,`preview_orig`,`preview_w`,`preview_h`,`media_filename`,`media_w`,`media_h`,`media_size`,`media_hash`,`media_orig`,`spoiler`,`deleted`,`capcode`,`email`,`name`,`trip`,`title`,`comment`,`delpass`,`sticky`,`locked`,`poster_hash`,`poster_country`,`exif`)
        SELECT *
        FROM (SELECT
                -- _id																'media_id',
                IF(unique_ips IS NULL, 0, unique_ips)							'poster_ip',	-- Unused in Asagi. Used in FF.
                no																'num',
                0																'subnum',		-- Unused in Asagi. Used in FF for ghost posts.
                IF(resto=0, no, resto)											'thread_num',
                IF(resto=0, TRUE, FALSE)										'op',
                `time`															'timestamp',
                0																'timestamp_expired',
                IF(tim IS NULL, NULL, CONCAT(tim, 's.jpg'))						'preview_orig',
                IF(tn_w IS NULL, 0, tn_w)										'preview_w',
                IF(tn_h IS NULL, 0, tn_h)										'preview_h',
                IF(filename IS NULL, NULL, CONCAT(filename, ext))				'media_filename',
                IF(w IS NULL, 0, w)												'media_w',
                IF(h IS NULL, 0, h)												'media_h',
                IF(fsize IS NULL, 0, h)											'media_size',
                md5																'media_hash',
                IF(tim IS NOT NULL and ext IS NOT NULL, CONCAT(tim, ext), NULL)	'media_orig',
                IF(spoiler IS NULL, FALSE, spoiler)								'spoiler',
                0																'deleted',
                IF(capcode='manager' or capcode='Manager', 'G', coalesce(upper(left(capcode, 1)),'N'))  'capcode',
                NULL															'email',
                doCleanFull(name)												'name',
                trip															'trip',
                doCleanFull(sub)												'title',
                doCleanFull(com)												'comment',
                NULL															'delpass',		-- Unused in Asagi. Used in FF.
                IFNULL(sticky, FALSE)								            'sticky',
                IF((closed IS not NULL or closed=1) and (archived is null or archived = 0), closed, false)     'locked',
                IF(id='Developer', 'Dev', id)									'poster_hash',	-- Not the same as media_hash
                IF(country is not null and (country='XX' or country='A1'), null, country)   'poster_country',
                -- country_name													'poster_country_name',
                NULLIF(cast(JSON_REMOVE(
                    JSON_OBJECT(
                    IF(unique_ips is null, 'null__', 'uniqueIps'), cast(unique_ips as char),
                    IF(since4pass is null, 'null__', 'since4pass'), cast(since4pass as char),
                    IF(country in('AC','AN','BL','CF','CM','CT','DM','EU','FC','GN','GY','JH','KN','MF','NB','NZ','PC','PR','RE','TM','TR','UN','WP'), 'trollCountry', 'null__' ), country), '$.null__') as char), '{{}}')    'exif' -- JSON in text format of uniqueIps, since4pass, and trollCountry. Has some deprecated fields but still used by Asagi and FF.
        FROM (
        SELECT * FROM JSON_TABLE(:jj, "$.posts[*]" COLUMNS (
                -- `_id`						FOR ORDINALITY,
                `no`				BIGINT		PATH "$.no",
                `sticky`			SMALLINT	PATH "$.sticky",
                `closed`			SMALLINT	PATH "$.closed",
                `now`				TEXT		PATH "$.now",
                `name`				TEXT		PATH "$.name",
                `sub`				TEXT		PATH "$.sub",
                `com`				TEXT		PATH "$.com",
                `filedeleted`		SMALLINT	PATH "$.filedeleted",
                `spoiler`			SMALLINT	PATH "$.spoiler",
                `custom_spoiler`	SMALLINT	PATH "$.custom_spoiler",
                `filename`			TEXT		PATH "$.filename",
                `ext`				TEXT		PATH "$.ext",
                `w`					INT			PATH "$.h",
                `h`					INT			PATH "$.w",
                `tn_w`				INT			PATH "$.tn_w",
                `tn_h`				INT			PATH "$.tn_h",
                `tim`				BIGINT		PATH "$.tim",
                `time`				BIGINT		PATH "$.time",
                `md5`				TEXT		PATH "$.md5",
                `fsize`				BIGINT		PATH "$.fsize",
                `m_img`				SMALLINT	PATH "$.m_img",
                `resto`				INT			PATH "$.resto",
                `trip`				TEXT		PATH "$.trip",
                `id`				TEXT		PATH "$.id",
                `capcode`			TEXT		PATH "$.capcode",
                `country`			TEXT		PATH "$.country",
                `country_name`		TEXT		PATH "$.country_name",
                `archived`			SMALLINT	PATH "$.archived",
                `bumplimit`			SMALLINT	PATH "$.bumplimit",
                `archived_on`		BIGINT		PATH "$.archived_on",
                `imagelimit`		SMALLINT	PATH "$.imagelimit",
                `semantic_url`		TEXT		PATH "$.semantic_url",
                `replies`			INT			PATH "$.replies",
                `images`			INT			PATH "$.images",
                `unique_ips`		INT			PATH "$.unique_ips",
                `tag`				TEXT		PATH "$.tag",
                `since4pass`		SMALLINT	PATH "$.since4pass")
            ) AS w) AS `4chan`) AS q
            ON DUPLICATE KEY UPDATE
                -- `poster_ip`		= values(`poster_ip`),
                `num`				= values(`num`),
                `subnum`			= values(`subnum`),
                `thread_num`		= values(`thread_num`),
                `op`				= values(`op`),
                `timestamp`			= values(`timestamp`),
                `timestamp_expired`	= values(`timestamp_expired`),
                `preview_orig`		= values(`preview_orig`),
                `preview_w`			= values(`preview_w`),
                `preview_h`			= values(`preview_h`),
                `media_filename`	= values(`media_filename`),
                `media_w`			= values(`media_w`),
                `media_h`			= values(`media_h`),
                `media_size`		= values(`media_size`),
                `media_hash`		= values(`media_hash`),
                `media_orig`		= values(`media_orig`),
                `spoiler`			= values(`spoiler`),
                `deleted`			= values(`deleted`),
                `capcode`			= values(`capcode`),
                `email`				= values(`email`),
                `name`				= values(`name`),
                `trip`				= values(`trip`),
                `title`				= values(`title`),
                `comment`			= values(`comment`),
                `delpass`			= values(`delpass`),
                `sticky`			= values(`sticky`),
                `locked`			= values(`locked`),
                `poster_hash`		= values(`poster_hash`),
                `poster_country`	= values(`poster_country`),
                `exif`				= values(`exif`);
        "#,
            board
        )
    }
}

// Attempt to fix MySQL's lack of concurrency
// [Locking rows in MySQL](https://is.gd/lzJG8M)
// [PostgreSQL Concurrency with MVCC](https://devcenter.heroku.com/articles/postgresql-concurrency)
#[async_trait]
impl QueriesExecutor<Statement, mysql_async::Row> for Pool {
    async fn init_schema(&self, schema: &str, engine: Database, charset: &str) -> Result<u64> {
        log::debug!("init_schema");
        let conn = self.get_conn().await?;
        conn.drop_query(&self.query_init_schema("", engine, charset)).await?;
        Ok(1)
    }

    async fn init_type(&self) -> Result<u64> {
        // Do nothing
        Ok(1)
    }

    async fn init_metadata(&self, engine: Database, charset: &str) -> Result<u64> {
        log::debug!("init_metadata");
        let conn = self.get_conn().await?;
        conn.drop_query(&self.query_init_metadata(engine, charset))
            .await
            .expect("Err creating metadata");
        Ok(1)
    }

    async fn init_board(
        &self, board: YotsubaBoard, engine: Database, charset: &str
    ) -> Result<u64> {
        log::debug!("init_board /{}/", board);
        let conn = self.get_conn().await?;
        conn.drop_query(&self.query_init_board(board, engine, charset)).await?;
        Ok(1)
    }

    async fn init_views(&self, board: YotsubaBoard) -> Result<u64> {
        // Do nothing
        Ok(1)
    }

    async fn update_metadata(
        &self, statements: &StatementStore<Statement>, endpoint: YotsubaEndpoint,
        board: YotsubaBoard, item: &[u8]
    ) -> Result<u64>
    {
        log::debug!("update_metadata");
        let mut conn = self.get_conn().await?;
        let json = &serde_json::from_slice::<serde_json::Value>(item)?;
        // let id = YotsubaIdentifier::new(endpoint, board, YotsubaStatement::UpdateMetadata);

        // Ok(statement
        //     .first(params! { "bb" => board.to_string(), "jj" => json })
        //     .await
        //     .map(|(c, r)| r)
        //     .ok()
        //     .flatten()
        //     .unwrap_or(1))
        conn = conn
            .drop_query(format!("SELECT *,1 from `metadata` WHERE board = '{}' for update;", board))
            .await?;
        Ok(conn
            .prep_exec(
                self.query_update_metadata(endpoint),
                params! { "bb" => board.to_string(), "jj" => json }
            )
            .await?
            .collect_and_drop()
            .await?
            .1
            .pop()
            .unwrap_or(1))
    }

    async fn update_thread(
        &self, statements: &StatementStore<Statement>, endpoint: YotsubaEndpoint,
        board: YotsubaBoard, item: &[u8]
    ) -> Result<u64>
    {
        log::debug!("update_thread");
        let mut conn = self.get_conn().await?;
        let json = &serde_json::from_slice::<serde_json::Value>(item)?;
        // let id = YotsubaIdentifier { endpoint, board, statement: YotsubaStatement::UpdateThread
        // Ok(statement
        //     .first(params! { "jj" => json })
        //     .await
        //     .map(|(c, r)| r)
        //     .ok()
        //     .flatten()
        //     .unwrap_or(1))

        // The result of this query will be empty since it's not SELECTing anything
        conn = conn.drop_query(format!("SELECT *,1 from `{}` limit 1 for update;", board)).await?;
        Ok(conn
            .first_exec(self.query_update_thread(board), params! {"jj" => json })
            .await
            .map(|(c, val)| val)?
            .unwrap_or(1))
    }

    async fn delete(
        &self, statements: &StatementStore<Statement>, endpoint: YotsubaEndpoint,
        board: YotsubaBoard, no: u32
    ) -> Result<u64>
    {
        log::debug!("delete");
        let conn = self.get_conn().await?;
        // let id = YotsubaIdentifier { endpoint, board, statement: YotsubaStatement::Delete };
        conn.drop_query(format!(
            "SELECT num, deleted, timestamp_expired,1 from `{}` WHERE num = {} for update;",
            board, no
        ))
        .await?
        .drop_exec(self.query_delete(board), (&i64::try_from(no)?,))
        .await?;
        Ok(1)

        //    .await
        //   .expect("Err executing sql: delete");
        // pool.disconnect().await?;
        // if let Some(_) = statement
        //     .first((i64::try_from(no)?,))
        //     .await
        //     .map(|(c, r): (mysql_async::Stmt<mysql_async::Conn>, Option<u64>)| r)
        //     .ok()
        //     .flatten()
        // {
        // };
    }

    async fn update_deleteds(
        &self, statements: &StatementStore<Statement>, endpoint: YotsubaEndpoint,
        board: YotsubaBoard, thread: u32, item: &[u8]
    ) -> Result<u64>
    {
        log::debug!("update_deleteds");
        let mut conn = self.get_conn().await?;
        let json = &serde_json::from_slice::<serde_json::Value>(item)?;
        // let id = YotsubaIdentifier::new(endpoint, board, YotsubaStatement::UpdateDeleteds);

        // Ok(statement
        //     .first(params! {"jj" => json, "no" => i64::try_from(thread)? })
        //     .await
        //     .map(|(c, r)| r)
        //     .ok()
        //     .flatten()
        //     .unwrap_or(1))
        conn = conn
            .drop_query(format!(
                "SELECT *,1 from `{}` where thread_num={} for update;",
                board, thread
            ))
            .await?;

        Ok(conn
            .prep_exec(
                self.query_update_deleteds(board),
                params! {"jj" => json, "no" => &i64::try_from(thread)? }
            )
            .await?
            .collect_and_drop()
            .await?
            .1
            .pop()
            .unwrap_or(1))
    }

    async fn update_hash(
        &self, statements: &StatementStore<Statement>, endpoint: YotsubaEndpoint,
        board: YotsubaBoard, no: u64, hash_type: YotsubaStatement, hashsum: Vec<u8>
    ) -> Result<u64>
    {
        // Do nothing
        Ok(1)
    }

    async fn medias(
        &self, statements: &StatementStore<Statement>, endpoint: YotsubaEndpoint,
        board: YotsubaBoard, no: u32
    ) -> Result<Vec<Row>>
    {
        log::debug!("medias");
        let conn = self.get_conn().await?;
        // let id = YotsubaIdentifier { endpoint, board, statement: YotsubaStatement::Medias };

        // Ok(Rows::MySQL(
        //     statement
        //         .execute(params! {"no" => i64::try_from(no)? })
        //         .await?
        //         .collect_and_drop()
        //         .await?
        //         .1
        // ))

        let r = Ok(conn
            .prep_exec(
                self.query_medias(board, YotsubaStatement::Medias),
                params! {"no" => &(no as i64)}
            )
            .await?
            .collect_and_drop()
            .await?
            .1);
        // log::warn!("medias finished {:?}", &r);
        // pool.disconnect().await?;
        r
    }

    async fn threads(
        &self, statements: &StatementStore<Statement>, endpoint: YotsubaEndpoint,
        board: YotsubaBoard, item: &[u8]
    ) -> Result<VecDeque<u32>>
    {
        log::debug!("threads");

        if matches!(endpoint, YotsubaEndpoint::Archive) {
            return Ok(serde_json::from_slice::<VecDeque<u32>>(item)?);
        }

        let conn = self.get_conn().await?;
        let json = serde_json::from_slice::<serde_json::Value>(item)?;
        // let id = YotsubaIdentifier { endpoint, board, statement: YotsubaStatement::Threads };
        Ok(conn
            .first_exec(self.query_threads(), params! { "jj"=>json})
            .await
            .map(|(c, val): (mysql_async::Conn, Option<Row>)| val)?
            .map(|r| r.get(0))
            .flatten()
            .map(|j: Option<serde_json::Value>| j)
            .flatten()
            .map(|j| serde_json::from_value::<VecDeque<Option<u32>>>(j))
            .ok_or_else(|| anyhow!("|threads| Empty or null in getting {}", endpoint))?
            .map(|v| v.into_iter().filter(Option::is_some).map(Option::unwrap).collect())?)
    }

    async fn threads_modified(
        &self, statements: &StatementStore<Statement>, endpoint: YotsubaEndpoint,
        board: YotsubaBoard, new_threads: &[u8]
    ) -> Result<VecDeque<u32>>
    {
        log::debug!("threads_modified");
        let conn = self.get_conn().await?;
        let json = serde_json::from_slice::<serde_json::Value>(new_threads)?;
        // let id = YotsubaIdentifier::new(YotsubaEndpoint::Threads, board,
        // YotsubaStatement::ThreadsModified);
        Ok(conn
            .first_exec(
                self.query_threads_modified(endpoint),
                params! {"bb" => &board.to_string(), "jj" => json}
            )
            .await
            .map(|(c, val): (mysql_async::Conn, Option<Row>)| val)?
            .map(|r| r.get(0))
            .flatten()
            .map(|j: Option<serde_json::Value>| j)
            .flatten()
            .map(|j| serde_json::from_value::<VecDeque<Option<u32>>>(j))
            .ok_or_else(|| anyhow!("|threads_modified| Empty or null in getting {}", endpoint))?
            .map(|v| v.into_iter().filter(Option::is_some).map(Option::unwrap).collect())?)
    }

    async fn threads_combined(
        &self, statements: &StatementStore<Statement>, endpoint: YotsubaEndpoint,
        board: YotsubaBoard, new_threads: &[u8]
    ) -> Result<VecDeque<u32>>
    {
        log::debug!("threads_combined");
        // let pool = mysql_async::Pool::from_url("mysql://root:zxc@localhost:3306/asagi")?;
        let conn = self.get_conn().await?;
        let json = serde_json::from_slice::<serde_json::Value>(new_threads)?;
        // let id = YotsubaIdentifier::new(YotsubaEndpoint::Threads, board,
        // YotsubaStatement::ThreadsCombined);
        Ok(conn
            .first_exec(
                self.query_threads_combined(board, endpoint),
                params! {"bb" => &board.to_string(), "jj" => json}
            )
            .await
            .map(|(c, val): (mysql_async::Conn, Option<Row>)| val)?
            .map(|r| r.get(0))
            .flatten()
            .map(|j: Option<serde_json::Value>| j)
            .flatten()
            .map(|j| serde_json::from_value::<VecDeque<Option<u32>>>(j))
            .ok_or_else(|| anyhow!("|threads_combined| Empty or null in getting {}", endpoint))?
            .map(|v| v.into_iter().filter(Option::is_some).map(Option::unwrap).collect())?)
    }

    async fn metadata(
        &self, statements: &StatementStore<Statement>, endpoint: YotsubaEndpoint,
        board: YotsubaBoard
    ) -> Result<bool>
    {
        log::debug!("inside metadata /{}/ {}", board, endpoint);
        let conn = self.get_conn().await?;
        // let id = YotsubaIdentifier { endpoint, board, statement: YotsubaStatement::Metadata };
        Ok(conn
            .prep_exec(self.query_metadata(endpoint), params! {"bb" => board.to_string()})
            .await
            .map(|x| x.collect_and_drop())?
            .await?
            .1
            .pop()
            .unwrap_or(false))

        // statement
        //     .first(params! { "bb" => &board.to_string() })
        //     .await
        //     .map(|(c, r)| r)
        //     .ok()
        //     .flatten()
        //     .unwrap_or(false)
    }
}

fn query_4chan_schema() -> String {
    format!(
        r#"SELECT * FROM JSON_TABLE(:jj, "$.posts[*]" COLUMNS(
        `no`				BIGINT		PATH "$.no",
        `sticky`			TINYINT  	PATH "$.sticky",
        `closed`			TINYINT  	PATH "$.closed",
        `now`				TEXT		PATH "$.now",
        `name`				TEXT		PATH "$.name",
        `sub`				TEXT		PATH "$.sub",
        `com`				TEXT		PATH "$.com",
        `filedeleted`		TINYINT  	PATH "$.filedeleted",
        `spoiler`			TINYINT 	PATH "$.spoiler",
        `custom_spoiler`	SMALLINT	PATH "$.custom_spoiler",
        `filename`			TEXT		PATH "$.filename",
        `ext`				TEXT		PATH "$.ext",
        `w`					INT			PATH "$.h",
        `h`					INT			PATH "$.w",
        `tn_w`				INT			PATH "$.tn_w",
        `tn_h`				INT			PATH "$.tn_h",
        `tim`				BIGINT		PATH "$.tim",
        `time`				BIGINT		PATH "$.time",
        `md5`				TEXT		PATH "$.md5",
        `fsize`				BIGINT		PATH "$.fsize",
        `m_img`				TINYINT	PATH "$.m_img",
        `resto`				BIGINT			PATH "$.resto",
        `trip`				TEXT		PATH "$.trip",
        `id`				TEXT		PATH "$.id",
        `capcode`			TEXT		PATH "$.capcode",
        `country`			TEXT		PATH "$.country",
        `country_name`		TEXT		PATH "$.country_name",
        `archived`			TINYINT    	PATH "$.archived",
        `bumplimit`			TINYINT   	PATH "$.bumplimit",
        `archived_on`		BIGINT		PATH "$.archived_on",
        `imagelimit`		SMALLINT	PATH "$.imagelimit",
        `semantic_url`		TEXT		PATH "$.semantic_url",
        `replies`			INT			PATH "$.replies",
        `images`			INT			PATH "$.images",
        `unique_ips`		INT			PATH "$.unique_ips",
        `tag`				TEXT		PATH "$.tag",
        `since4pass`		SMALLINT	PATH "$.since4pass")
        ) w
    "#
    )
}

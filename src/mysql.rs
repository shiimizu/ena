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
use enum_iterator::IntoEnumIterator;
use mysql_async::{prelude::*, Pool, Row};
use std::{
    boxed::Box,
    collections::HashSet,
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

impl QueriesNew for Pool {
    fn inquiry(&self, statement: YotsubaStatement, id: QueryIdentifier) -> String {
        match statement {
            YotsubaStatement::InitSchema => format!(
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
                engine = id.engine.mysql_engine(),
                charset = id.charset.unwrap()
            ),
            YotsubaStatement::InitType => "select 1".into(),
            YotsubaStatement::InitMetadata => format!(
                "
                CREATE TABLE IF NOT EXISTS `metadata` (
                    `board` varchar(10) NOT NULL PRIMARY key unique ,
                    `threads` json,
                    `archive` json,
                    INDEX metadata_board_idx (`board`)
                ) ENGINE={engine} CHARSET={charset} COLLATE={charset}_general_ci;
                ",
                engine = id.engine.mysql_engine(),
                charset = id.charset.unwrap()
            ),
            YotsubaStatement::InitBoard => format!(
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
                board = id.board,
                engine = id.engine.mysql_engine(),
                charset = id.charset.unwrap()
            ),
            YotsubaStatement::InitViews => "select 1".into(),
            YotsubaStatement::UpdateMetadata => format!(
                r#"
                INSERT INTO `metadata`(`board`, `{endpoint}`)       
                SELECT :bb, :jj
                ON DUPLICATE KEY update
                   `{endpoint}` = :jj;"#,
                endpoint = id.endpoint
            ),
            YotsubaStatement::UpdateThread => format!(
                r#"
            INSERT INTO `{board}`(`poster_ip`,`num`,`subnum`,`thread_num`,`op`,`timestamp`,`timestamp_expired`,`preview_orig`,`preview_w`,`preview_h`,`media_filename`,`media_w`,`media_h`,`media_size`,`media_hash`,`media_orig`,`spoiler`,`deleted`,`capcode`,`email`,`name`,`trip`,`title`,`comment`,`delpass`,`sticky`,`locked`,`poster_hash`,`poster_country`,`exif`)
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
                    IFNULL(IF(country is not null and (country='XX' or country='A1'), null, country), troll_country)   'poster_country',
                    -- country_name													'poster_country_name',
                    NULLIF(cast(JSON_REMOVE(
                        JSON_OBJECT(
                        IF(unique_ips is null, 'null__', 'uniqueIps'), cast(unique_ips as char),
                        IF(since4pass is null, 'null__', 'since4pass'), cast(since4pass as char),
                        IF(country or troll_country in('AC','AN','BL','CF','CM','CT','DM','EU','FC','GN','GY','JH','KN','MF','NB','NZ','PC','PR','RE','TM','TR','UN','WP'), 'trollCountry', 'null__' ), IFNULL(country, troll_country)), '$.null__') as char), '{{}}')    'exif' -- JSON in text format of uniqueIps, since4pass, and trollCountry. Has some deprecated fields but still used by Asagi and FF.
            FROM ( {schema_4chan_query} ) AS `4chan`) AS q
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
                board = id.board,
                schema_4chan_query = query_4chan_schema()
            ),
            YotsubaStatement::Delete => format!(
                "UPDATE `{}` SET deleted = 1, timestamp_expired = unix_timestamp() WHERE num = ? AND subnum = 0",
                id.board
            ),

            // This is not used as MySQL takes too long processing this.
            // So it is done locally with a HashSet
            YotsubaStatement::UpdateDeleteds => format!(
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
                board = id.board,
                schema_4chan_query = query_4chan_schema()
            ),
            YotsubaStatement::UpdateHashMedia => "select 1".into(),
            YotsubaStatement::UpdateHashThumbs => "select 1".into(),
            YotsubaStatement::Medias => format!(
                "SELECT * FROM `{board}`
                    WHERE (media_hash is not null) AND (num=:no or thread_num=:no)
                    ORDER BY num desc LOCK IN SHARE MODE;",
                board = id.board
            ),
            YotsubaStatement::Threads => r#"
            SELECT JSON_ARRAYAGG(no)
            FROM
            ( SELECT * FROM JSON_TABLE(:jj, "$[*].threads[*]" COLUMNS(
            `no`				bigint		PATH "$.no")
            ) w )z
            WHERE no is not null LOCK IN SHARE MODE;
            "#
            .to_string(),

            // This is not used as MySQL takes too long processing this.
            // So it is done locally with a HashSet
            YotsubaStatement::ThreadsModified => {
                let threads = r#"
            select JSON_ARRAYAGG(c) from (
                select prev->'$.no' as c from (
                SELECT prev from metadata m2 ,
                    JSON_TABLE(threads, '$[*].threads[*]'
                    COLUMNS(prev json path '$')) q 
                LEFT OUTER JOIN
                (SELECT newv from 
                    JSON_TABLE(:jj, '$[*].threads[*]'
                    COLUMNS(newv json path '$')) w) e
                on prev->'$.no' = newv->'$.no'
                where board = :bb and newv is null or prev is null or prev->'$.last_modified' != newv->'$.last_modified'
                
                UNION
                
                SELECT prev from metadata m3 ,
                    JSON_TABLE( threads, '$[*].threads[*]'
                    COLUMNS(prev json path '$')) r
                RIGHT OUTER JOIN
                (SELECT newv FROM
                    JSON_TABLE(:jj, '$[*].threads[*]'
                    COLUMNS(newv json path '$')) a) s
                on prev->'$.no' = newv->'$.no'
                where board = :bb and newv is null or prev is null or prev->'$.last_modified' != newv->'$.last_modified'
            ) z )i where c is not null;
        "#.to_string();
                let archive = r#"
            SELECT JSON_ARRAYAGG(c) from (
                select prev as c (
                SELECT prev from metadata m2 ,
                    JSON_TABLE( archive, '$[*]'
                    COLUMNS(prev json path '$')) q 
                LEFT OUTER JOIN
                (SELECT newv from 
                    JSON_TABLE(:jj, '$[*]'
                    COLUMNS(newv json path '$')) w) e
                on prev = newv
                where board = :bb
                
                UNION
                
                SELECT prev from metadata m3 ,
                    JSON_TABLE( archive, '$[*]'
                    COLUMNS(prev json path '$')) r
                RIGHT OUTER JOIN
                (SELECT newv FROM
                    JSON_TABLE(:jj, '$[*]'
                    COLUMNS(newv json path '$')) a) s
                on prev = newv
                where board = :bb
            ) z
            where newv is null or prev is null
            )xx where c is not null;
            "#
                .to_string();
                match id.endpoint {
                    YotsubaEndpoint::Threads => threads,
                    _ => archive
                }
            }

            // This is not used as MySQL takes too long processing this.
            // So it is done locally with a HashSet
            YotsubaStatement::ThreadsCombined => {
                let thread = format!(
                    r#"
                select JSON_ARRAYAGG(c) from (
                    select c from (
                    SELECT prev->'$.no' as c from (
                        SELECT prev from metadata m2 ,
                            JSON_TABLE(threads, '$[*].threads[*]'
                            COLUMNS(prev json path '$')) q 
                        LEFT OUTER JOIN
                        (SELECT newv from 
                            JSON_TABLE(:jj, '$[*].threads[*]'
                            COLUMNS(newv json path '$')) w) e
                        on prev->'$.no' = newv->'$.no'
                        where board = :bb
                        
                        UNION
                        
                        SELECT prev from metadata m3 ,
                            JSON_TABLE( threads, '$[*].threads[*]'
                            COLUMNS(prev json path '$')) r
                        RIGHT OUTER JOIN
                        (SELECT newv FROM
                            JSON_TABLE(:jj, '$[*].threads[*]'
                            COLUMNS(newv json path '$')) a) s
                        on prev->'$.no' = newv->'$.no'
                        where board = :bb
                    ) z ) i
                    left join
                      (select num as nno from `{board}` where op=1 and (timestamp_expired is not null or deleted is not null))u
                      ON c = nno
                    where nno is null LOCK IN SHARE MODE
                    )xx where c is not null;
                "#,
                    board = id.board
                );

                let archive = format!(
                    r#"
                select JSON_ARRAYAGG(c) from (
                    select c from (
                    SELECT coalesce (newv, prev) as c from (
                        SELECT prev from metadata m2 ,
                            JSON_TABLE(archive, '$[*]'
                            COLUMNS(prev json path '$')) q 
                        LEFT OUTER JOIN
                        (SELECT newv from 
                            JSON_TABLE(:jj, '$[*]'
                            COLUMNS(newv json path '$')) w) e
                        on prev = newv
                        where board = :bb
                        
                        UNION
                        
                        SELECT prev from metadata m3 ,
                            JSON_TABLE(archive, '$[*]'
                            COLUMNS(prev json path '$')) r
                        RIGHT OUTER JOIN
                        (SELECT newv FROM
                            JSON_TABLE(:jj, '$[*]'
                            COLUMNS(newv json path '$')) a) s
                        on prev = newv
                        where board = :bb
                    ) z ) i
                    left join
                      (select num as nno from `{board}` where op=1 and (timestamp_expired is not null or deleted is not null))u
                      ON c = nno
                    where  nno is null LOCK IN SHARE MODE
                    )xx where c is not null;
                "#,
                    board = id.board
                );
                match id.endpoint {
                    YotsubaEndpoint::Archive => archive,
                    _ => thread
                }
            }
            YotsubaStatement::Metadata => format!(
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
                endpoint = id.endpoint,
                path1 = if matches!(id.endpoint, YotsubaEndpoint::Threads) {
                    "$[*].threads[*]"
                } else {
                    "$[*]"
                },
                path2 = if matches!(id.endpoint, YotsubaEndpoint::Threads) { "$.no" } else { "$" }
            )
        }
    }
}

#[async_trait]
impl QueriesExecutorNew<Statement, Row> for Pool {
    async fn first(
        &self, statement: YotsubaStatement, id: &QueryIdentifier,
        statements: &StatementStore<Statement>, item: Option<&[u8]>, no: Option<u64>
    ) -> Result<u64>
    {
        log::debug!("|QueriesExecutorNew| Running: {}", statement);
        let endpoint = id.endpoint;
        let board = id.board;
        let mut conn = self.get_conn().await?;

        let item = item.ok_or_else(|| {
            anyhow!("|QueriesExecutorNew::{}| Empty `json` item received", statement)
        });
        let no =
            no.ok_or_else(|| anyhow!("|QueriesExecutorNew::{}| Empty `no` received", statement));
        match statement {
            YotsubaStatement::InitSchema
            | YotsubaStatement::InitMetadata
            | YotsubaStatement::InitBoard => {
                conn.drop_query(&self.inquiry(statement, id.clone())).await?;
                Ok(1)
            }
            YotsubaStatement::Metadata => Ok(conn
                .prep_exec(
                    self.inquiry(statement, id.clone()),
                    params! {"bb" => id.board.to_string()}
                )
                .await
                .map(|x| x.collect_and_drop())?
                .await?
                .1
                .pop()
                .unwrap_or(0)),
            YotsubaStatement::UpdateMetadata => {
                let json = serde_json::from_slice::<serde_json::Value>(item?)?;
                conn = conn
                    .drop_exec(
                        "SELECT *,1 from `metadata` WHERE board = ? for update;",
                        (&board.to_string(),)
                    )
                    .await?;
                Ok(conn
                    .first_exec(
                        self.inquiry(statement, id.clone()),
                        params! { "bb" => board.to_string(), "jj" => json }
                    )
                    .await
                    .map(|(c, val)| val)?
                    .unwrap_or(1))
            }
            YotsubaStatement::UpdateThread => {
                // The result of this query will be empty since it's not SELECTing anything
                let json = serde_json::from_slice::<serde_json::Value>(item?)?;
                conn = conn
                    .drop_query(format!("SELECT *,1 from `{}` limit 1 for update;", board))
                    .await?;
                Ok(conn
                    .first_exec(self.inquiry(statement, id.clone()), params! {"jj" => json })
                    .await
                    .map(|(c, val)| val)?
                    .unwrap_or(1))
            }
            YotsubaStatement::Delete => {
                let no = no?;
                conn.drop_query(format!(
                    "SELECT num, deleted, timestamp_expired,1 from `{}` WHERE num = {} for update;",
                    board, no
                ))
                .await?
                .drop_exec(self.inquiry(statement, id.clone()), (&i64::try_from(no)?,))
                .await?;
                Ok(1)
            }

            YotsubaStatement::UpdateDeleteds => {
                // {thread}.json
                // get posts from db
                // compare that with fetched posts
                // mark deleted - the ones missing in fetched posts
                let q: Thread = serde_json::from_slice(item?)?;
                let new: Queue = q.posts.into_iter().map(|post| post.no).collect();
                let min = new.iter().min().ok_or_else(|| {
                    anyhow!("|QueriesExecutorNew::{}| Empty `min` from threads", statement)
                })?;
                // q.min();
                let no = no?;
                let (conn, val) : (mysql_async::Conn, Option<serde_json::Value>) = conn
                    .first_exec(format!(
                        "SELECT JSON_ARRAYAGG(num) from `{board}` where thread_num=? and thread_num >= ?;",
                        board = board
                    ), (no, min))
                    .await?;
                let val = val.ok_or_else(|| {
                    anyhow!("|QueriesExecutorNew::{}| Empty `json` item received", statement)
                });
                if val.is_err() {
                    // Here, the threads diff return no changes, meaning no posts are deleted
                    return Ok(1);
                }

                let orig: Queue = serde_json::from_value(val?)?;
                let diff: HashSet<_> = orig.difference(&new).collect();
                if diff.is_empty() {
                    // Here, the threads diff return no changes, meaning no posts are deleted
                    return Ok(1);
                }
                log::info!("min: {} thread_num: {} diff: {:?}", min, no, diff);

                Ok(conn
                    .first_exec(
                        format!(
                            r#"
                            UPDATE `{board}`
                            SET deleted = 1
                            where num in (
                                    SELECT no from
                                    JSON_TABLE(:jj,     "$[*]" COLUMNS(
                                    `no`				bigint		PATH "$")) z);"#,
                            board = id.board
                        ),
                        params! {"jj" => serde_json::to_value(&diff)? }
                    )
                    .await
                    // This will return empty since we're not selecting anything, so we can return a
                    // code of 1.
                    .map(|(c, val): (mysql_async::Conn, Option<u64>)| val)?
                    .unwrap_or(1))
            }
            YotsubaStatement::InitType
            | YotsubaStatement::InitViews
            | YotsubaStatement::UpdateHashMedia
            | YotsubaStatement::UpdateHashThumbs => Ok(1),
            // YotsubaStatement::Medias => {},
            // YotsubaStatement::Threads => {},
            // YotsubaStatement::ThreadsModified => {},
            // YotsubaStatement::ThreadsCombined => {},
            _ => Err(anyhow!("|QueriesExecutorNew| Unknown statement: {}", statement))
        }
    }

    async fn get_list(
        &self, statement: YotsubaStatement, id: &QueryIdentifier,
        statements: &StatementStore<Statement>, item: Option<&[u8]>, no: Option<u64>
    ) -> Result<Queue>
    {
        log::debug!("|QueriesExecutorNew| Running: {}", statement);
        if !matches!(
            statement,
            YotsubaStatement::Threads
                | YotsubaStatement::ThreadsModified
                | YotsubaStatement::ThreadsCombined
        ) {
            return Err(anyhow!(
                "|QueriesExecutorNew::{}| Unknown statement: {}",
                YotsubaStatement::Threads,
                statement
            ));
        }
        let id = QueryIdentifier { media_mode: statement, ..id.clone() };
        let endpoint = id.endpoint;
        let board = id.board;
        let conn = self.get_conn().await?;
        let item = item.ok_or_else(|| {
            anyhow!("|QueriesExecutorNew::{}| Empty `json` item received", statement)
        });
        if matches!(endpoint, YotsubaEndpoint::Archive) {
            let u: Queue = serde_json::from_slice(item?)?;
            return Ok(conn
                .first_exec(
                    format!("select `{}` from metadata where board = '{}'", endpoint, board),
                    ()
                )
                .await
                .map(|(c, val): (mysql_async::Conn, Option<Row>)| val)?
                .map(|r| r.get(0))
                .flatten()
                .map(|j: Option<serde_json::Value>| j)
                .flatten()
                .map(|j| serde_json::from_value::<Queue>(j))
                .ok_or_else(|| {
                    anyhow!(
                        "|QueriesExecutorNew::{}| Empty or null in getting {}",
                        statement,
                        endpoint
                    )
                })?
                .map(|t: Queue| match statement {
                    YotsubaStatement::Threads => u,
                    YotsubaStatement::ThreadsModified =>
                        t.symmetric_difference(&u).map(|&s| s).collect(),
                    _ => t.union(&u).map(|&s| s).collect()
                })?);
        }
        let threads: Vec<Threads> = serde_json::from_slice(item?)?;
        Ok(conn
            .first_exec(
                format!("select `{}` from metadata where board = '{}'", endpoint, board),
                ()
            )
            .await
            .map(|(c, val): (mysql_async::Conn, Option<Row>)| val)?
            .map(|r| r.get(0))
            .flatten()
            .map(|j: Option<serde_json::Value>| j)
            .flatten()
            .map(|j| serde_json::from_value::<ThreadsList>(j))
            .ok_or_else(|| {
                anyhow!("|QueriesExecutorNew::{}| Empty or null in getting {}", statement, endpoint)
            })?
            .map(|t: ThreadsList| match statement {
                YotsubaStatement::Threads => Ok(t.to_queue()),
                YotsubaStatement::ThreadsModified => t.symmetric_difference(endpoint, &threads),
                _ => t.union(endpoint, &threads)
            })??)
    }

    async fn get_rows(
        &self, statement: YotsubaStatement, id: &QueryIdentifier,
        statements: &StatementStore<Statement>, item: Option<&[u8]>, no: Option<u64>
    ) -> Result<Vec<Row>>
    {
        log::debug!("|QueriesExecutorNew| Running: {}", statement);
        if !matches!(statement, YotsubaStatement::Medias) {
            return Err(anyhow!(
                "|QueriesExecutorNew::{}| Unknown statement: {}",
                YotsubaStatement::Medias,
                statement
            ));
        }
        let id = QueryIdentifier { media_mode: statement, ..id.clone() };
        let conn = self.get_conn().await?;
        let no =
            no.ok_or_else(|| anyhow!("|QueriesExecutorNew::{}| Empty `no` received", statement));

        Ok(conn
            .prep_exec(
                self.inquiry(statement, id.clone()),
                // self.query_medias(id.board, id.media_mode),
                params! {"no" => no? as i64}
            )
            .await?
            .collect_and_drop()
            .await?
            .1)
    }

    async fn create_statements(
        &self, engine: Database, endpoint: YotsubaEndpoint, board: YotsubaBoard
    ) -> StatementStore<Statement> {
        // Can't use statments for Asagi and MySQL due to the way [`mysql_async`] is implemented.
        // Connections don't hold a shared reference and statements can only be run once because it
        // ALSO moves itself out... wtf.. Therefore new connections need to be taken from
        // the pool, and statments are always constanly being remade for each query..
        // Here just give a placeholder statement.
        std::collections::HashMap::new()
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
        `troll_country`		TEXT		PATH "$.troll_country",
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

// https://stackoverflow.com/questions/34662713/how-can-i-create-parameterized-tests-in-rust
// [`mysql_async`] only returns library or server errors. Queries such as /INSERT/DELETE
#[cfg(test)]
mod test {

    use super::*;
    use crate::enums::{YotsubaBoard, YotsubaHash};
    use once_cell::sync::Lazy;
    #[allow(unused_imports)]
    #[cfg(test)]
    use pretty_assertions::{assert_eq, assert_ne};
    use serde_json::json;
    static BOARD: Lazy<YotsubaBoard> = Lazy::new(|| YotsubaBoard::a);
    const DB_URL: &str = "mysql://root:@localhost:3306/asagi";
    const SCHEMA: &str = "asagi";
    const ENGINE: Database = Database::MySQL;
    // const CHARSET: &str = "utf8";

    enum JsonType {
        Unknown,
        Valid,
        DeprecatedFields,
        AddedFields,
        MixedFields
    }

    macro_rules! send_thread_tests {
        ($($name:ident: $value:expr,)*) => {
            $(
                #[tokio::test]
                async fn $name() -> Result<()> {
                    let ( endpoint, board, mode, json_type) = $value;
                    test_send_single_thread(endpoint, board, mode, json_type).await?;
                    // assert_eq!(expected, fib(input));
                    Ok(())
                }
            )*
        }
    }
    macro_rules! get_threads_tests {
        ($($name:ident: $value:expr,)*) => {
            $(
                #[tokio::test]
                // #[should_panic(expected = "|threads| Empty or null in getting threads")] // for unknowns
                async fn $name() -> Result<()> {
                    let ( endpoint, board, mode, json_type) = $value;
                    test_get_threads(endpoint, board, mode, json_type).await?;
                    // assert_eq!(expected, fib(input));
                    Ok(())
                }
            )*
        }
    }

    macro_rules! get_threads_tests_panic {
        ($($name:ident: $value:expr,)*) => {
            $(
                // #[should_panic(expected = "|threads| Empty or null in getting threads")] // for unknowns
                #[tokio::test]
                #[should_panic]
                async fn $name() {
                    let (endpoint, board, mode, json_type) = $value;
                    test_get_threads(endpoint, board, mode, json_type).await.unwrap();
                    // assert_eq!(expected, fib(input));
                }
            )*
        }
    }
    #[cfg(test)]
    send_thread_tests! {
        // Send single thread
        send_single_thread_valid_json: (YotsubaEndpoint::Threads, *BOARD, YotsubaStatement::UpdateThread, JsonType::Valid),
        send_single_thread_deprecated_json: (YotsubaEndpoint::Threads, *BOARD, YotsubaStatement::UpdateThread, JsonType::DeprecatedFields),
        send_single_thread_added_json: (YotsubaEndpoint::Threads, *BOARD, YotsubaStatement::UpdateThread, JsonType::AddedFields),
        send_single_thread_mixed_json: (YotsubaEndpoint::Threads, *BOARD, YotsubaStatement::UpdateThread, JsonType::MixedFields),
    }

    #[cfg(test)]
    get_threads_tests! {
        // threads.json
        get_threads_send_valid_json: (YotsubaEndpoint::Threads, *BOARD, YotsubaStatement::Threads, JsonType::Valid),

        get_threads_modified_send_valid_json: (YotsubaEndpoint::Threads, *BOARD, YotsubaStatement::ThreadsModified, JsonType::Valid),

        get_threads_combined_send_valid_json: (YotsubaEndpoint::Threads, *BOARD, YotsubaStatement::ThreadsCombined, JsonType::Valid),

        get_threads_send_added_json: (YotsubaEndpoint::Threads, *BOARD, YotsubaStatement::Threads, JsonType::AddedFields),
        get_threads_modified_send_added_json: (YotsubaEndpoint::Threads, *BOARD, YotsubaStatement::ThreadsModified, JsonType::AddedFields),
        get_threads_combined_send_added_json: (YotsubaEndpoint::Threads, *BOARD, YotsubaStatement::ThreadsCombined, JsonType::AddedFields),

        // archive.json
        get_archive_send_valid_json: (YotsubaEndpoint::Archive, *BOARD, YotsubaStatement::Threads, JsonType::Valid),

        get_archive_modified_send_valid_json: (YotsubaEndpoint::Archive, *BOARD, YotsubaStatement::ThreadsModified, JsonType::Valid),
        get_archive_modified_send_deprecated_json: (YotsubaEndpoint::Archive, *BOARD, YotsubaStatement::ThreadsModified, JsonType::DeprecatedFields),

        get_archive_combined_send_valid_json: (YotsubaEndpoint::Archive, *BOARD, YotsubaStatement::ThreadsCombined, JsonType::Valid),
        get_archive_combined_send_deprecated_json: (YotsubaEndpoint::Archive, *BOARD, YotsubaStatement::ThreadsCombined, JsonType::DeprecatedFields),



    }

    #[cfg(test)]
    get_threads_tests_panic! {
        get_threads_send_unknown_json: (YotsubaEndpoint::Threads, *BOARD, YotsubaStatement::Threads, JsonType::Unknown),
        get_threads_send_deprecated_json: (YotsubaEndpoint::Threads, *BOARD, YotsubaStatement::Threads, JsonType::DeprecatedFields),
        get_threads_modified_send_deprecated_json: (YotsubaEndpoint::Threads, *BOARD, YotsubaStatement::ThreadsModified, JsonType::DeprecatedFields),
        get_threads_combined_send_deprecated_json: (YotsubaEndpoint::Threads, *BOARD, YotsubaStatement::ThreadsCombined, JsonType::DeprecatedFields),

        get_threads_send_mixed_json: (YotsubaEndpoint::Threads, *BOARD, YotsubaStatement::Threads, JsonType::MixedFields),

        get_threads_modified_send_unknown_json: (YotsubaEndpoint::Threads, *BOARD, YotsubaStatement::ThreadsModified, JsonType::Unknown),
        get_threads_combined_send_unknown_json: (YotsubaEndpoint::Threads, *BOARD, YotsubaStatement::ThreadsCombined, JsonType::Unknown),

        get_threads_modified_send_mixed_json: (YotsubaEndpoint::Threads, *BOARD, YotsubaStatement::ThreadsModified, JsonType::MixedFields),

        get_threads_combined_send_mixed_json: (YotsubaEndpoint::Threads, *BOARD, YotsubaStatement::ThreadsCombined, JsonType::MixedFields),

        get_archive_send_added_json: (YotsubaEndpoint::Archive, *BOARD, YotsubaStatement::Threads, JsonType::AddedFields),
        get_archive_send_mixed_json: (YotsubaEndpoint::Archive, *BOARD, YotsubaStatement::Threads, JsonType::MixedFields),

        get_archive_modified_send_added_json: (YotsubaEndpoint::Archive, *BOARD, YotsubaStatement::ThreadsModified, JsonType::AddedFields),
        get_archive_modified_send_mixed_json: (YotsubaEndpoint::Archive, *BOARD, YotsubaStatement::ThreadsModified, JsonType::MixedFields),

        get_archive_combined_send_added_json: (YotsubaEndpoint::Archive, *BOARD, YotsubaStatement::ThreadsCombined, JsonType::AddedFields),
        get_archive_combined_send_mixed_json: (YotsubaEndpoint::Archive, *BOARD, YotsubaStatement::ThreadsCombined, JsonType::MixedFields),
    }

    async fn test_get_threads(
        endpoint: YotsubaEndpoint, board: YotsubaBoard, mode: YotsubaStatement, json_type: JsonType
    ) -> Result<Queue> {
        let _json = match json_type {
            JsonType::Unknown =>
                if endpoint == YotsubaEndpoint::Threads {
                    json!({"test":1, "test2":2, "test3":3})
                } else {
                    json!(["1243234", "5645756", "75686786", "456454325", "test", "test1"])
                },
            JsonType::Valid =>
                if endpoint == YotsubaEndpoint::Threads {
                    json!(
                    [
                        {
                          "page": 1,
                          "threads": [
                            { "no": 196649146, "last_modified": 1576266882, "replies": 349 },
                            { "no": 196656555, "last_modified": 1576266881, "replies": 6 },
                            { "no": 196654076, "last_modified": 1576266880, "replies": 191 },
                            { "no": 196637792, "last_modified": 1576266880, "replies": 233 },
                            { "no": 196647457, "last_modified": 1576266880, "replies": 110 },
                            { "no": 196624742, "last_modified": 1576266873, "replies": 103 },
                            { "no": 196656097, "last_modified": 1576266868, "replies": 7 },
                            { "no": 196645355, "last_modified": 1576266866, "replies": 361 },
                            { "no": 196655995, "last_modified": 1576266867, "replies": 3 },
                            { "no": 196655998, "last_modified": 1576266860, "replies": 5 },
                            { "no": 196652782, "last_modified": 1576266858, "replies": 42 },
                            { "no": 196656536, "last_modified": 1576266853, "replies": 5 },
                            { "no": 196621039, "last_modified": 1576266853, "replies": 189 },
                            { "no": 196640441, "last_modified": 1576266851, "replies": 495 },
                            { "no": 196637247, "last_modified": 1576266850, "replies": 101 }
                          ]
                        },
                        {
                          "page": 2,
                          "threads": [
                            { "no": 196650664, "last_modified": 1576266846, "replies": 29 },
                            { "no": 196646963, "last_modified": 1576266845, "replies": 387 },
                            { "no": 196648390, "last_modified": 1576266844, "replies": 36 },
                            { "no": 196651494, "last_modified": 1576266832, "replies": 10 },
                            { "no": 196656773, "last_modified": 1576266827, "replies": 0 },
                            { "no": 196653207, "last_modified": 1576266827, "replies": 20 },
                            { "no": 196643737, "last_modified": 1576266825, "replies": 82 },
                            { "no": 196626714, "last_modified": 1576266824, "replies": 467 },
                            { "no": 196654299, "last_modified": 1576266821, "replies": 9 },
                            { "no": 196636729, "last_modified": 1576266819, "replies": 216 },
                            { "no": 196655015, "last_modified": 1576266819, "replies": 3 },
                            { "no": 196642084, "last_modified": 1576266818, "replies": 233 },
                            { "no": 196649533, "last_modified": 1576266816, "replies": 122 },
                            { "no": 196640416, "last_modified": 1576266806, "replies": 381 },
                            { "no": 196656724, "last_modified": 1576266794, "replies": 1 }
                          ]
                        }
                    ])
                } else {
                    json!([1243234, 5645756, 75686786, 456454325, 231412, 564576567, 34523234])
                },
            JsonType::DeprecatedFields =>
                if endpoint == YotsubaEndpoint::Threads {
                    json!(
                    [
                        {
                          "page": 1,
                          "threads": [
                            { "no_more": 196649146, "last_modified": 1576266882, "replies": 349 },
                            { "no_more": 196656555, "last_modified": 1576266881, "replies": 7 }
                          ]
                        },
                        {
                          "page": 2,
                          "threads": [
                            { "no_more": 196650664, "last_modified": 1576266846, "replies": 387},
                            { "no_more": 196646963, "last_modified": 1576266845, "replies": 487 }
                          ]
                        }
                    ])
                } else {
                    json!([])
                },
            JsonType::AddedFields =>
                if endpoint == YotsubaEndpoint::Threads {
                    json!(
                    [
                        {
                          "page": 1,
                          "threads": [
                            { "no": 196649146, "last_modified": 1576266882, "replies": 349, "new_field": 349 },
                            { "no": 196656555, "last_modified": 1576266881, "replies": 6,  "new_field": 7 }
                          ]
                        },
                        {
                          "page": 2,
                          "threads": [
                            { "no": 196650664, "last_modified": 1576266846, "replies": 387, "new_field": 387 },
                            { "no": 196646963, "last_modified": 1576266845, "replies": 487, "new_field": 487 },
                            { "no": 196648390, "last_modified": 1576266844, "replies": 36 , "new_field": 36 }
                          ]
                        }
                    ])
                } else {
                    json!([{}, "1243234", "5645756", "75686786", "456454325", "test", "test1"])
                },
            JsonType::MixedFields =>
                if endpoint == YotsubaEndpoint::Threads {
                    json!(
                    [
                        {
                          "page": 1,
                          "threads": [
                            { "no": 196649146, "last_modified": 1576266882, "replies": 349, "new_field": 349 },
                            { "no_more": 196656555, "last_modified": 1576266881, "replies": 6,  "new_field": 7 }
                          ]
                        },
                        {
                          "page": 2,
                          "threads": [
                            { "no": 196650664, "last_modified": 1576266846, "replies": 387, "new_field": 387 },
                            { "no": 196646963, "last_modified": 1576266845, "replies": 487, "new_field": 487 },
                            { "no_more": 196648390, "last_modified": 1576266844, "replies": 36 , "new_field": 36 }
                          ]
                        }
                    ])
                } else {
                    json!(["1243234", 5645756, "75686786", 456454325, "test", "test1"])
                },
        };

        let json = serde_json::to_vec(&_json).unwrap();

        let pool = Pool::new(DB_URL);

        let statements = pool.create_statements(ENGINE, endpoint, board).await;

        // This is the ID used by `create_statements` for the get`threads_*` variants.
        let id = &QueryIdentifier::new(
            ENGINE,
            endpoint,
            board,
            None,
            None,
            YotsubaHash::Sha256,
            YotsubaStatement::Medias
        );

        match mode {
            YotsubaStatement::Threads
            | YotsubaStatement::ThreadsModified
            | YotsubaStatement::ThreadsCombined =>
                Ok(pool.get_list(mode, &id, &statements, Some(json.as_slice()), None).await?),
            _ => Err(anyhow!("Error. Entered this test with : `{}` statement", mode))
        }
    }
    async fn test_send_single_thread(
        endpoint: YotsubaEndpoint, board: YotsubaBoard, mode: YotsubaStatement, json_type: JsonType
    ) -> Result<u64> {
        let _json = match json_type {
            JsonType::Unknown => json!({"test":1, "test2":2, "test3":3}),
            JsonType::Valid => json!(
                {
                    "posts": [{
                      "no": 5679879,
                      "sticky": 1,
                      "closed": 1,
                      "now": r#"12\/31\/18(Mon)17:05:48"#,
                      "name": "Anonymous",
                      "sub": r#"Welcome to \/po\/!"#,
                      "com": r#"Welcome to \/po\/! We specialize in origami, papercraft, and everything that\u2019s relevant to paper engineering. This board is also an great library of relevant PDF books and instructions, one of the best resource of its kind on the internet.<br><br>Questions and discussions of papercraft and origami are welcome. Threads for topics covered by paper engineering in general are also welcome, such as kirigami, bookbinding, printing technology, sticker making, gift boxes, greeting cards, and more.<br><br>Requesting is permitted, even encouraged if it\u2019s a good request; fulfilled requests strengthens this board\u2019s role as a repository of books and instructions. However do try to keep requests in relevant threads, if you can.<br><br>\/po\/ is a slow board! Do not needlessly bump threads."#,
                      "filename": "yotsuba_folding",
                      "ext": ".png",
                      "w": 530,
                      "h": 449,
                      "tn_w": 250,
                      "tn_h": 211,
                      "tim": 1546293948883 as i64,
                      "time": 1546293948,
                      "md5": "uZUeZeB14FVR+Mc2ScHvVA==",
                      "fsize": 516657,
                      "resto": 0,
                      "capcode": "mod",
                      "semantic_url": "welcome-to-po",
                      "replies": 2,
                      "images": 2,
                      "unique_ips": 1
                    }]}
            ),
            JsonType::DeprecatedFields => json!(
                {
                    "posts": [{
                      "no": 4588723,
                      "sticky": 1,
                      "closed": 1,
                      "ext": ".png",
                      "resto": 123457,
                      "w": 530,
                      "h": 449,
                      "tn_w": 250,
                      "tn_h": 211,
                      "tim": 1546293948883 as i64,
                      "time": 1546293948,
                      "unique_ips": 1
                    }]}
            ),
            JsonType::AddedFields => json!(
                {
                    "posts": [{
                      "no": 462537,
                      "sticky": 1,
                      "closed": 1,
                      "now": r#"12\/31\/18(Mon)17:05:48"#,
                      "name": "Anonymous",
                      "sub": "Wrg!",
                      "com": "sdf",
                      "filename": "yotsuba_folding",
                      "ext": ".png",
                      "w": 530,
                      "h": 449,
                      "tn_w": 250,
                      "tn_h": 211,
                      "tim": 1546293948883 as i64,
                      "time": 1546293948,
                      "md5": "uZUeZeB14FVR+Mc2ScHvVA==",
                      "fsize": 516657,
                      "resto": 0,
                      "capcode": "mod",
                      "semantic_url": "welcome-to-po",
                      "replies": 2,
                      "images": 2,
                      "unique_ips": 1,
                      "test": 1,
                      "added": 1
                    }]}
            ),
            JsonType::MixedFields => json!(
                {
                    "posts": [{
                      "no": 6745672,
                      "sticky": 1,
                      "closed": 1,
                      "ext": ".png",
                      "w": 530,
                      "h": 449,
                      "tn_w": 250,
                      "resto": 123457,
                      "tn_h": 211,
                      "tim": 1546293948883 as i64,
                      "time": 1546293948,
                      "unique_ips": 1,
                      "test": 1,
                      "test2": 1
                    }]}
            )
        };

        let json = serde_json::to_vec(&_json).unwrap();

        let pool = Pool::new(DB_URL);

        let statements = pool.create_statements(ENGINE, endpoint, board).await;

        // This is the ID used by `create_statements` for the get`threads_*` variants.
        let id = &QueryIdentifier::new(
            ENGINE,
            endpoint,
            board,
            None,
            None,
            YotsubaHash::Sha256,
            YotsubaStatement::Medias
        );

        match mode {
            YotsubaStatement::UpdateThread =>
                Ok(pool.first(mode, &id, &statements, Some(json.as_slice()), None).await?),
            _ => Err(anyhow!("Error. Entered this test with : `{}` statement", mode))
        }
    }
}

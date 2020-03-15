#![allow(dead_code)]
#![allow(unused_variables)]
#![allow(unused_imports)]
use crate::{sql::*, YotsubaBoard, YotsubaEndpoint, YotsubaHash};
use ::mysql::{prelude::*, Statement, *};
use anyhow::{anyhow, Result};
use async_trait::async_trait;
use std::{
    collections::{HashMap, VecDeque},
    convert::TryFrom
};

impl Queries2 for ::mysql::Pool {
    fn query_init_schema(&self, schema: &str) -> String {
        // unreachable!()
        "".into()
    }

    fn query_init_metadata(&self) -> String {
        format!(
            "
            CREATE TABLE IF NOT EXISTS metadata (
                board VARCHAR(10) NOT NULL PRIMARY key UNIQUE,
                `threads` json,
                `archive` json,
                INDEX metadata_board_idx (board)
            ) ENGINE=InnoDB CHARSET=utf8mb4 COLLATE=utf8mb4_general_ci;
            "
        )
    }

    fn query_delete(&self, board: YotsubaBoard) -> String {
        format!(
            "UPDATE `{}` SET deleted = 1, timestamp_expired = unix_timestamp() WHERE num = ? AND subnum = 0",
            board
        )
    }

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
        unreachable!()
    }

    fn query_update_metadata(&self, column: YotsubaEndpoint) -> String {
        format!(
            "INSERT INTO metadata(board, {0})
                  VALUES (:board, :json)
                  ON CONFLICT (board)
                  DO UPDATE
                      SET {0} = :json;
              ",
            column
        )
    }

    fn query_medias(&self, board: YotsubaBoard, thumb: YotsubaStatement) -> String {
        format!(
            "SELECT * FROM `{0}`
                WHERE (media_hash is not null) AND (num=:no or thread_num=:no)
                ORDER BY num desc;",
            board
        )
    }

    fn query_threads_modified(&self, endpoint: YotsubaEndpoint) -> String {
        let thread = format!(
            r#"
        SELECT JSON_ARRAYAGG(coalesce (newv->'$.no', prev->'$.no')) from (
            SELECT * from metadata m2 ,
                JSON_TABLE(threads, '$[*].threads[*]'
                COLUMNS(prev json path '$')) q 
            LEFT OUTER JOIN
            (SELECT * from 
                JSON_TABLE(:json, '$[*].threads[*]'
                COLUMNS(newv json path '$')) w) e
            on prev->'$.no' = newv->'$.no'
            where board = :board
            
            UNION
            
            SELECT * from metadata m3 ,
                JSON_TABLE( threads, '$[*].threads[*]'
                COLUMNS(prev json path '$')) r
            RIGHT OUTER JOIN
            (SELECT * FROM
                JSON_TABLE(:json, '$[*].threads[*]'
                COLUMNS(newv json path '$')) a) s
            on prev->'$.no' = newv->'$.no'
            where board = :board
        ) z
        where newv is null or prev is null or prev->'$.last_modified' != newv->'$.last_modified';
        "#
        );

        let archive = format!(
            r#"
        SELECT JSON_ARRAYAGG(coalesce (newv,prev)) from (
            SELECT * from metadata m2 ,
                JSON_TABLE( archive, '$[*]'
                COLUMNS(prev json path '$')) q 
            LEFT OUTER JOIN
            (SELECT * from 
                JSON_TABLE(:json, '$[*]'
                COLUMNS(newv json path '$')) w) e
            on prev = newv
            where board = :board
            
            UNION
            
            SELECT * from metadata m3 ,
                JSON_TABLE( archive, '$[*]'
                COLUMNS(prev json path '$')) r
            RIGHT OUTER JOIN
            (SELECT * FROM
                JSON_TABLE(:json, '$[*]'
                COLUMNS(newv json path '$')) a) s
            on prev = newv
            where board = :board
        ) z
        where newv is null or prev is null;
        "#
        );

        match endpoint {
            YotsubaEndpoint::Archive => archive,
            _ => thread
        }
    }

    fn query_threads(&self) -> String {
        format!(
            r#"
        SELECT JSON_ARRAYAGG(z.no)
        FROM
        ( {schema_4chan_query} )z
        "#,
            schema_4chan_query = query_4chan_schema()
        )
    }

    fn query_metadata(&self, column: YotsubaEndpoint) -> String {
        format!(
            r#"SELECT CASE WHEN {} IS NOT null THEN true ELSE false END FROM metadata WHERE board = ?"#,
            column
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
                    JSON_TABLE(:json, '$[*].threads[*]'
                    COLUMNS(newv json path '$')) w) e
                on prev->'$.no' = newv->'$.no'
                where board = :board
                
                UNION
                
                SELECT * from metadata m3 ,
                    JSON_TABLE( threads, '$[*].threads[*]'
                    COLUMNS(prev json path '$')) r
                RIGHT OUTER JOIN
                (SELECT * FROM
                    JSON_TABLE(:json, '$[*].threads[*]'
                    COLUMNS(newv json path '$')) a) s
                on prev->'$.no' = newv->'$.no'
                where board = :board
            ) z ) i
            left join
              (select num as nno from `{}` where op=1 and (timestamp_expired is not null or deleted is not null))u
              ON c = nno
            where  nno is null;
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
                    JSON_TABLE(:json, '$[*]'
                    COLUMNS(newv json path '$')) w) e
                on prev = newv
                where board = :board
                
                UNION
                
                SELECT * from metadata m3 ,
                    JSON_TABLE(archive, '$[*]'
                    COLUMNS(prev json path '$')) r
                RIGHT OUTER JOIN
                (SELECT * FROM
                    JSON_TABLE(:json, '$[*]'
                    COLUMNS(newv json path '$')) a) s
                on prev = newv
                where board = :board
            ) z ) i
            left join
              (select num as nno from `{}` where op=1 and (timestamp_expired is not null or deleted is not null))u
              ON c = nno
            where  nno is null;
        "#,
            board
        );
        match endpoint {
            YotsubaEndpoint::Archive => archive,
            _ => thread
        }
    }

    fn query_init_board(&self, board: YotsubaBoard) -> String {
        r#"CREATE TABLE IF NOT EXISTS `?` (
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
          ) engine=InnoDB CHARSET=utf8mb4 COLLATE=utf8mb4_general_ci;"#
            .to_string()
    }

    fn query_init_type(&self) -> String {
        unreachable!()
    }

    fn query_init_views(&self, board: YotsubaBoard) -> String {
        unreachable!()
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
                IF(resto=0, resto, no)											'thread_num',
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
                IF(capcode IS NULL, 'N', capcode)								'capcode',
                NULL															'email',
                doCleanFull(name)												'name',
                trip															'trip',
                doCleanFull(sub)												'title',
                doCleanFull(com)												'comment',
                NULL															'delpass',		-- Unused in Asagi. Used in FF.
                IF(sticky IS NULL, FALSE, sticky)								'sticky',
                IF(closed IS NULL, FALSE, closed)								'locked',
                IF(id='Developer', 'Dev', id)									'poster_hash',	-- Not the same as media_hash
                country															'poster_country',
                -- country_name													'poster_country_name',
                -- CAST(JSON_OBJECT('uniqueIps', unique_ips, 'since4pass', since4pass, 'trollCountry', 'NULL') AS CHAR) 'exif' -- JSON in text format of uniqueIps, since4pass, and trollCountry. Has some deprecated fields but still used by Asagi and FF.
                NULL 'exif'
        FROM (
        SELECT * FROM JSON_TABLE(@j, "$.posts[*]" COLUMNS (
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

#[async_trait]
impl QueriesExecutor2<Statement> for ::mysql::Pool {
    async fn init_schema_new(&self, schema: &str) {
        unimplemented!()
    }

    async fn init_type_new(&self) {
        unimplemented!()
    }

    async fn init_metadata_new(&self) {
        unimplemented!()
    }

    async fn init_board_new(&self, board: YotsubaBoard) {
        unimplemented!()
    }

    async fn init_views_new(&self, board: YotsubaBoard) {
        unimplemented!()
    }

    async fn update_metadata_new(
        &self, statements: &StatementStore2<Statement>, endpoint: YotsubaEndpoint,
        board: YotsubaBoard, item: &[u8]
    ) -> Result<u64>
    {
        unimplemented!()
    }

    async fn update_thread_new(
        &self, statements: &StatementStore2<Statement>, endpoint: YotsubaEndpoint,
        board: YotsubaBoard, item: &[u8]
    ) -> Result<u64>
    {
        unimplemented!()
    }

    async fn delete_new(
        &self, statements: &StatementStore2<Statement>, endpoint: YotsubaEndpoint,
        board: YotsubaBoard, no: u32
    )
    {
        unimplemented!()
    }

    async fn update_deleteds_new(
        &self, statements: &StatementStore2<Statement>, endpoint: YotsubaEndpoint,
        board: YotsubaBoard, thread: u32, item: &[u8]
    ) -> Result<u64>
    {
        unimplemented!()
    }

    async fn update_hash_new(
        &self, statements: &StatementStore2<Statement>, endpoint: YotsubaEndpoint,
        board: YotsubaBoard, no: u64, hash_type: YotsubaStatement, hashsum: Vec<u8>
    )
    {
        unimplemented!()
    }

    async fn medias_new(
        &self, statements: &StatementStore2<Statement>, endpoint: YotsubaEndpoint,
        board: YotsubaBoard, no: u32
    ) -> Result<Rows>
    {
        unimplemented!()
    }

    async fn threads_new(
        &self, statements: &StatementStore2<Statement>, endpoint: YotsubaEndpoint,
        board: YotsubaBoard, item: &[u8]
    ) -> Result<VecDeque<u32>>
    {
        unimplemented!()
    }

    async fn threads_modified_new(
        &self, board: YotsubaBoard, new_threads: &[u8], statement: &Statement
    ) -> Result<VecDeque<u32>> {
        unimplemented!()
    }

    async fn threads_combined_new(
        &self, statements: &StatementStore2<Statement>, endpoint: YotsubaEndpoint,
        board: YotsubaBoard, new_threads: &[u8]
    ) -> Result<VecDeque<u32>>
    {
        unimplemented!()
    }

    async fn metadata_new(
        &self, statements: &StatementStore2<Statement>, endpoint: YotsubaEndpoint,
        board: YotsubaBoard
    ) -> bool
    {
        unimplemented!()
    }
}

fn query_4chan_schema() -> String {
    format!(
        r#"SELECT * FROM JSON_TABLE(?, "$.posts[*]" COLUMNS(
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
        ) w
    "#
    )
}

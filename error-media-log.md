
> Why am I not getting the same amount of media in my database as on my filesystem?

# A case study in correct thumbnails

Here are some useful SQL queries:

```sql
-- get thumbnails

SELECT COUNT(*) FROM (SELECT DISTINCT sha256t FROM media where sha256t is not null) AS temp;


-- get post for tha same md5 hash from a searching a sha256 thumb hash 

SELECT posts.resto, posts.no, posts.tim, posts.ext, posts.filename, encode(media.md5,'hex') as md5 from posts
INNER JOIN media ON posts.md5 = media.md5
where posts.md5 is not null and media.sha256t is not null and encode(sha256t,'hex')='1d60995006c9d92461e055da6d5cd5e07480e35d7fc80e425184316e85455911';
```

**TLDR**:   
* Both of these situations are due to OP|REPLY thumbnail  
* They're both the same full media md5, but their thumbnails depend on whether post is `OP` or `THUMB`   
* Furthermore, due to our schema of having all boards in one table, we'll have to account for `SFW`|`NSFW` thumbnails (which affect a transparent image's thumbnail to be blue or orange) to be able to check if we have the thumbnail before downloading.

Diff from in filesystem vs in-db. Omitted `.jpg` extension. These files are found in FS but not in db:    
* `1d60995006c9d92461e055da6d5cd5e07480e35d7fc80e425184316e85455911`
* `c0a6d5c31839d6245d75d705b20b482cf32d7c76137aff2506bd796586f32d4b`

---

<br>

**First let's take a look at: `1d60995006c9d92461e055da6d5cd5e07480e35d7fc80e425184316e85455911`**

Log output:
```
download_media: /a/207632530#207632530 067383a10eea20794f81cf7fde04f07b | assets/media_ena_a_test_thumbs3/thumbnails/1/91/1d60995006c9d92461e055da6d5cd5e07480e35d7fc80e425184316e85455911.jpg
```

Log output:
```
download_media: /a/207628802#207632249 067383a10eea20794f81cf7fde04f07b | assets/media_ena_a_test_thumbs3/thumbnails/4/77/71a51a26f618d71ab9f14b14f4024c48f8a1224b1481512979626f1962a2a774.jpg
```

As you can see they have the same `md5`: `067383a10eea20794f81cf7fde04f07b`  
Also you can see one is OP, the other is a REPLY

----------------------

<br>

**Next let's take a look at: `c0a6d5c31839d6245d75d705b20b482cf32d7c76137aff2506bd796586f32d4b`**


Log output:
```
download_media: /a/207635927#207635927 23403f8dc2cec85bcc136c736dec6bc2 | assets/media_ena_a_test_thumbs3/thumbnails/b/d4/c0a6d5c31839d6245d75d705b20b482cf32d7c76137aff2506bd796586f32d4b.jpg
```

SQL query output:
```
ena=# select * from media where md5='\x23403f8dc2cec85bcc136c736dec6bc2';
 banned | id |                md5                 | sha256 |                              sha256t                               | content | content_thumb
--------+----+------------------------------------+--------+--------------------------------------------------------------------+---------+---------------
        |    | \x23403f8dc2cec85bcc136c736dec6bc2 |        | \xbbae8c36a97c41e32b7e94eb7d871234684c8bc00022634b9c87270a3fcbf57e |         |
```

Log output:
```
download_media: /a/207567202#207637317 23403f8dc2cec85bcc136c736dec6bc2 | assets/media_ena_a_test_thumbs3/thumbnails/e/57/bbae8c36a97c41e32b7e94eb7d871234684c8bc00022634b9c87270a3fcbf57e.jpg
```

As you can see they have the same `md5`: `23403f8dc2cec85bcc136c736dec6bc2`   
Also you can see one is OP, the other is a REPLY

see : `my-file.txt` (program log output) (btw it's not pushed to the repo)

--------------------

<br>

# Conditions
* different md5 - same thumbnails for both (thumbgen)

* same md5 - different thumbnails (happens if one is OP image, the other is reply image. Their thumbnails will be different)  

.. basically one md5 can have max 4 type of thumbnails `SFW`|`NSFW` board + `OP`|`REPLY`   

I think it's not possible to check thumbnails before dl becuase there can be 4 permutations of it. Where would you read/store the data?   

Apparently getting thumbnails is hard and theres no way to check before downloading them unless you store the hash for all 4 permutations  

the frontend needs to grab the right thumb hash OR generate the thumbs  
but it can't generate the thumbs if it doesnt have full media  
but if youre not downloading full media and downloading thumbnails, then you need a way to dedup them  

because asagi can afford to since it has an images table for each board and only needs to keep track of `OP`|`REPLY`, we don't even need to specify `SFW`|`NSFW`. It only dedups by board, versus our `media` table which dedups all boards, which needs more maintenance and correctness to account for dumping everything into one table.   

Also it doesn't help that they can change their algorithm for generating thumbs, invalidating your corpus of thumbnail hashes, meaning you'll re-download them everytime they change........  

... this is all if you want the HASH of the thumbnail though.  

but if you don't care, just use a unique filename like what Asagi does: `123453345s.jpg`  
so you can always check it regardless of algorithm changes.  

If you go that route but still use hashes as the filename, if the algorithm changes, the filename & the actual hash might be confusing to deal with, since they'll be different.  

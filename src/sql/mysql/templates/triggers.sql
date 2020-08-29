DROP PROCEDURE IF EXISTS `update_thread_%%BOARD%%`;

CREATE PROCEDURE `update_thread_%%BOARD%%` (tnum INT, ghost_num INT, p_timestamp INT,
  p_media_hash VARCHAR(25), p_email VARCHAR(100), p_sticky INT, p_locked INT)
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
    `%%BOARD%%_threads` op
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
    ),
    op.sticky = (
      COALESCE(p_sticky, op.sticky)
    ),
    op.locked = (
      COALESCE(p_locked, op.locked)
    )
    WHERE op.thread_num = tnum;
END;

DROP PROCEDURE IF EXISTS `update_thread_timestamp_%%BOARD%%`;

CREATE PROCEDURE `update_thread_timestamp_%%BOARD%%` (tnum INT, timestamp INT)
BEGIN
  UPDATE
    `%%BOARD%%_threads` op
  SET
    op.time_last_modified = (
      GREATEST(op.time_last_modified, timestamp)
    )
  WHERE op.thread_num = tnum;
END;

DROP PROCEDURE IF EXISTS `create_thread_%%BOARD%%`;

CREATE PROCEDURE `create_thread_%%BOARD%%` (num INT, timestamp INT)
BEGIN
  INSERT IGNORE INTO `%%BOARD%%_threads` VALUES (num, timestamp, timestamp,
    timestamp, NULL, NULL, timestamp, 0, 0, 0, 0);
END;

DROP PROCEDURE IF EXISTS `delete_thread_%%BOARD%%`;

CREATE PROCEDURE `delete_thread_%%BOARD%%` (tnum INT)
BEGIN
  DELETE FROM `%%BOARD%%_threads` WHERE thread_num = tnum;
END;

DROP PROCEDURE IF EXISTS `insert_image_%%BOARD%%`;

CREATE PROCEDURE `insert_image_%%BOARD%%` (n_media_hash VARCHAR(25),
 n_media VARCHAR(20), n_preview VARCHAR(20), n_op INT)
BEGIN
  IF n_op = 1 THEN
    INSERT INTO `%%BOARD%%_images` (media_hash, media, preview_op, total)
    VALUES (n_media_hash, n_media, n_preview, 1)
    ON DUPLICATE KEY UPDATE
      media_id = LAST_INSERT_ID(media_id),
      total = (total + 1),
      preview_op = COALESCE(preview_op, VALUES(preview_op)),
      media = COALESCE(media, VALUES(media));
  ELSE
    INSERT INTO `%%BOARD%%_images` (media_hash, media, preview_reply, total)
    VALUES (n_media_hash, n_media, n_preview, 1)
    ON DUPLICATE KEY UPDATE
      media_id = LAST_INSERT_ID(media_id),
      total = (total + 1),
      preview_reply = COALESCE(preview_reply, VALUES(preview_reply)),
      media = COALESCE(media, VALUES(media));
  END IF;
END;

DROP PROCEDURE IF EXISTS `delete_image_%%BOARD%%`;

CREATE PROCEDURE `delete_image_%%BOARD%%` (n_media_id INT)
BEGIN
  UPDATE `%%BOARD%%_images` SET total = (total - 1) WHERE media_id = n_media_id;
END;

DROP PROCEDURE IF EXISTS `insert_post_%%BOARD%%`;

CREATE PROCEDURE `insert_post_%%BOARD%%` (p_timestamp INT, p_media_hash VARCHAR(25),
  p_email VARCHAR(100), p_name VARCHAR(100), p_trip VARCHAR(25))
BEGIN
  DECLARE d_day INT;
  DECLARE d_image INT;
  DECLARE d_sage INT;
  DECLARE d_anon INT;
  DECLARE d_trip INT;
  DECLARE d_name INT;

  SET d_day = FLOOR(p_timestamp/86400)*86400;
  SET d_image = p_media_hash IS NOT NULL;
  SET d_sage = COALESCE(p_email = 'sage', 0);
  SET d_anon = COALESCE(p_name = 'Anonymous' AND p_trip IS NULL, 0);
  SET d_trip = p_trip IS NOT NULL;
  SET d_name = COALESCE(p_name <> 'Anonymous' AND p_trip IS NULL, 1);

  INSERT INTO `%%BOARD%%_daily` VALUES(d_day, 1, d_image, d_sage, d_anon, d_trip,
    d_name)
    ON DUPLICATE KEY UPDATE posts=posts+1, images=images+d_image,
    sage=sage+d_sage, anons=anons+d_anon, trips=trips+d_trip,
    names=names+d_name;

  IF (SELECT trip FROM `%%BOARD%%_users` WHERE trip = p_trip) IS NOT NULL THEN
    UPDATE `%%BOARD%%_users` SET postcount=postcount+1,
        firstseen = LEAST(p_timestamp, firstseen),
        name = COALESCE(p_name, '')
      WHERE trip = p_trip;
  ELSE
    INSERT INTO `%%BOARD%%_users` VALUES(
    NULL, COALESCE(p_name,''), COALESCE(p_trip,''), p_timestamp, 1)
    ON DUPLICATE KEY UPDATE postcount=postcount+1,
      firstseen = LEAST(VALUES(firstseen), firstseen),
      name = COALESCE(p_name, '');
  END IF;
END;


DROP PROCEDURE IF EXISTS `delete_post_%%BOARD%%`;

CREATE PROCEDURE `delete_post_%%BOARD%%` (p_timestamp INT, p_media_hash VARCHAR(25), p_email VARCHAR(100), p_name VARCHAR(100), p_trip VARCHAR(25))
BEGIN
  DECLARE d_day INT;
  DECLARE d_image INT;
  DECLARE d_sage INT;
  DECLARE d_anon INT;
  DECLARE d_trip INT;
  DECLARE d_name INT;

  SET d_day = FLOOR(p_timestamp/86400)*86400;
  SET d_image = p_media_hash IS NOT NULL;
  SET d_sage = COALESCE(p_email = 'sage', 0);
  SET d_anon = COALESCE(p_name = 'Anonymous' AND p_trip IS NULL, 0);
  SET d_trip = p_trip IS NOT NULL;
  SET d_name = COALESCE(p_name <> 'Anonymous' AND p_trip IS NULL, 1);

  UPDATE `%%BOARD%%_daily` SET posts=posts-1, images=images-d_image,
    sage=sage-d_sage, anons=anons-d_anon, trips=trips-d_trip,
    names=names-d_name WHERE day = d_day;

  IF (SELECT trip FROM `%%BOARD%%_users` WHERE trip = p_trip) IS NOT NULL THEN
    UPDATE `%%BOARD%%_users` SET postcount = postcount-1 WHERE trip = p_trip;
  ELSE
    UPDATE `%%BOARD%%_users` SET postcount = postcount-1 WHERE
      name = COALESCE(p_name, '') AND trip = COALESCE(p_trip, '');
  END IF;
END;


DROP TRIGGER IF EXISTS `before_ins_%%BOARD%%`;

CREATE TRIGGER `before_ins_%%BOARD%%` BEFORE INSERT ON `%%BOARD%%`
FOR EACH ROW
BEGIN
  IF NEW.media_hash IS NOT NULL THEN
    CALL insert_image_%%BOARD%%(NEW.media_hash, NEW.media_orig, NEW.preview_orig, NEW.op);
    SET NEW.media_id = LAST_INSERT_ID();
  END IF;
END;

DROP TRIGGER IF EXISTS `after_ins_%%BOARD%%`;

CREATE TRIGGER `after_ins_%%BOARD%%` AFTER INSERT ON `%%BOARD%%`
FOR EACH ROW
BEGIN
  IF NEW.op = 1 THEN
    CALL create_thread_%%BOARD%%(NEW.num, NEW.timestamp);
  END IF;
  CALL update_thread_%%BOARD%%(NEW.thread_num, NEW.subnum, NEW.timestamp, NEW.media_hash, NEW.email, NEW.sticky, NEW.locked);
  CALL insert_post_%%BOARD%%(NEW.timestamp, NEW.media_hash, NEW.email, NEW.name, NEW.trip);
END;

DROP TRIGGER IF EXISTS `after_del_%%BOARD%%`;

CREATE TRIGGER `after_del_%%BOARD%%` AFTER DELETE ON `%%BOARD%%`
FOR EACH ROW
BEGIN
  CALL update_thread_%%BOARD%%(OLD.thread_num, OLD.subnum, OLD.timestamp, OLD.media_hash, OLD.email, OLD.sticky, OLD.locked);
  IF OLD.op = 1 THEN
    CALL delete_thread_%%BOARD%%(OLD.num);
  END IF;
  CALL delete_post_%%BOARD%%(OLD.timestamp, OLD.media_hash, OLD.email, OLD.name, OLD.trip);
  IF OLD.media_hash IS NOT NULL THEN
    CALL delete_image_%%BOARD%%(OLD.media_id);
  END IF;
END;

CREATE TRIGGER `after_upd_%%BOARD%%` AFTER UPDATE ON `%%BOARD%%`
FOR EACH ROW
BEGIN
  IF NEW.timestamp_expired <> 0 THEN
    CALL update_thread_timestamp_%%BOARD%%(NEW.thread_num, NEW.timestamp_expired);
  END IF;
END;

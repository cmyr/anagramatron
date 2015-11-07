drop table if exists hits;
CREATE TABLE hits (
    hit_id integer primary key,
    hit_status text not null,
    hit_date integer not null,
    hit_hash text unique not null,
    tweet_one text not null,
    tweet_two text not null);

CREATE INDEX hit_index ON hits (hit_hash);

drop table if exists post_queue;
CREATE TABLE post_queue (hit_id integer);

drop table if exists hitinfo;
CREATE TABLE hitinfo (last_post REAL);

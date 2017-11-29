drop table tweets;
drop table collisions;
drop table locations;
create table locations (
  location    VARCHAR(100) PRIMARY KEY,
  coordinates GEOGRAPHY
);

create table collisions(
  collisionID serial PRIMARY KEY,
  location varchar(100) REFERENCES locations,
  collisionTime TIMESTAMP,
  UNIQUE (location, collisionTime)
);

create table tweets(
  tweetID varchar(20),
  collisionID serial REFERENCES collisions,
  PRIMARY KEY (tweetID, collisionID)
);



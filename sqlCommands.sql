create database index_db;
use index_db;

create table urls(
	idurl int not null auto_increment,
	url varchar(500) not null,
    constraint pk_urls_idurl primary key (idurl)
);
create index idx_urls_url on urls (url);

create table words(
	idword int not null auto_increment,
    word varchar(200) not null,
    constraint pk_words_word primary key (idword)
);
create index idx_words_word on words (word);

create table word_location (wordsword_location
	idword_location int not null auto_increment,
    idurl int not null,
    idword int not null,
    location int,
    constraint pk_idword_location primary key (idword_location),
    constraint fk_word_location_idurl foreign key (idurl) references urls (idurl),
    constraint fk_word_location_idword foreign key (idword) references words (idword)
);
create index idx_word_location_idword on word_location (idword);

alter database index_db character set = utf8mb4 collate = utf8mb4_unicode_ci;
alter table words convert to character set utf8mb4 collate utf8mb4_unicode_ci;
alter table words modify column word varchar(200) character set utf8mb4 collate utf8mb4_unicode_ci;
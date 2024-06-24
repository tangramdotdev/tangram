#![allow(dead_code)]

use tangram_database as db;
use tangram_either::Either;

pub type Error = db::either::Error<db::sqlite::Error, db::postgres::Error>;

pub type Options = Either<db::sqlite::Options, db::postgres::Options>;

pub type Database = Either<db::sqlite::Database, db::postgres::Database>;

pub type Connection = Either<db::sqlite::Connection, db::postgres::Connection>;

pub type Transaction<'a> = Either<db::sqlite::Transaction<'a>, db::postgres::Transaction<'a>>;

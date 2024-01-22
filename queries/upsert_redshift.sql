BEGIN;
CREATE TABLE stg_books (
	book_id BIGINT,
	book_name VARCHAR(150),
	author_name VARCHAR(150),
	country_name VARCHAR(150),
	insertion_date DATE 
);

COPY stg_books 
FROM :s3path
iam_role 'arn:aws:iam::905418016947:role/service-role/AmazonRedshift-CommandsAccessRole-20240122T135740' 
FORMAT AS PARQUET;

DELETE FROM books
USING stg_books
WHERE books.book_id = stg_books.book_id;

INSERT INTO books
SELECT book_id, book_name, author_name, country_name, insertion_date from stg_books;

DROP TABLE IF EXISTS stg_books;

COMMIT;
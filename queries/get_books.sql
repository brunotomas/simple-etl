select b.id as book_id, b.name as book_name, a.name as author_name, c.name as country_name, b.inserted_at as insertion_date from book b
join author a
on b.author_id = a.id
join country c
on c.id = a.country_id
where inserted_at > %s;
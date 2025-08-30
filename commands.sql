create table users (
    id int primary key,
    name text not null,
    active bool default true
);

create table orders (
    id int primary key,
    user_id int references users(id),
    amount int not null
);

insert into users values (1, "Alice", true);
insert into users values (2, "Bob", false);
insert into users(id, name) values (3, "Charlie");

select * from users;

insert into orders values (1, 1, 100);
insert into orders values (2, 1, 200);
insert into orders values (3, 2, 150);

select id, name from users where active = true;

update users set active = true where id = 2;

delete from users where id = 3;

select u.name, o.amount
from users as u
inner join orders as o on u.id = o.user_id;

select u.name, o.amount
from users as u
left join orders as o on u.id = o.user_id;

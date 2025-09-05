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

create index users_id_idx on users(id);
create index users_name_idx on users(name);
create index orders_user_id_idx on orders(user_id);

insert into users values (1, "Alice", true);
insert into users values (2, "Bob", false);
insert into users(id, name) values (3, "Charlie");
insert into users(id, name) values (4, "Diana");
insert into users(id, name) values (5, "Eve");

insert into orders values (1, 1, 100);
insert into orders values (2, 1, 200);
insert into orders values (3, 2, 150);
insert into orders values (4, 4, 300);

begin;
insert into users values (6, "TempUser", true);
rollback;
select * from users where id = 6;

begin;
insert into users values (7, "Frank", true);
commit;
select * from users where id = 7;

begin;
delete from users where id = 5;
rollback;
select * from users where id = 5;

begin;
update users set name = "Ghost" where id = 4;
rollback;
select * from users where id = 4;

update users set active = true where id = 2;
update users set name = "Alicia" where id = 1;
update orders set amount = 250 where id = 2;
update users set active = false, name = "Charles" where id = 3;

select * from users;
select * from orders;

begin;
delete from users where id = 3;
rollback;
select * from users where id = 3;

begin;
delete from users where id = 3;
commit;
select * from users where id = 3;

select u.name, o.amount
from users as u
inner join orders as o on u.id = o.user_id;

select u.name, o.amount
from users as u
left join orders as o on u.id = o.user_id;

create index users_id_name_idx on users(id, name);
select * from users where id = 1 and name = "Alicia";

select * from users where id > 2;
select * from users where id >= 2 and id <= 4;
select * from users where id < 5;

select * from users where active = true;
select * from users where id = 2 and active = true;

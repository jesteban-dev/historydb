CREATE TABLE "public"."users" (
    "id"          BIGSERIAL, /* Bigint no funciona -> Solucionar */
    "username"    VARCHAR(16) NOT NULL,
    "email"       VARCHAR(50) NOT NULL,
    "name"        VARCHAR(16) NOT NULL,
    "surname"     VARCHAR(50),
    CONSTRAINT "users_pk" PRIMARY KEY ("id"),
    CONSTRAINT "users_username_uk" UNIQUE ("username"),
    CONSTRAINT "users_email_uk" UNIQUE ("email")
);
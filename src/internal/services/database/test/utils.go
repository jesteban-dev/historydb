package test

import (
	"fmt"
	"net/url"
	"strings"
)

func parseDatabaseURL(dbUrl string) (string, error) {
	dbUrl = strings.TrimSuffix(dbUrl, "?")

	u, err := url.Parse(dbUrl)
	if err != nil {
		return "", err
	}

	password, _ := u.User.Password()
	dbname := strings.TrimPrefix(u.Path, "/")

	q := u.Query()
	sslmode := q.Get("sslmode")
	if sslmode == "" {
		sslmode = "disable"
	}

	dsn := fmt.Sprintf("host=%s port=%s user=%s password=%s dbname=%s sslmode=%s", u.Hostname(), u.Port(), u.User.Username(), password, dbname, sslmode)
	return dsn, err
}

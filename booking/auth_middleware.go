// auth_middleware.go
package main

import (
	"context"
	"net/http"
	"strings"

	"github.com/golang-jwt/jwt/v5"
)

type ctxKey string

const ClaimsContextKey ctxKey = "claims"

func jwtAuthMiddleware(next http.Handler) http.Handler {
	secret := []byte(getenv("JWT_SECRET", "supersecret"))
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		h := r.Header.Get("Authorization")
		if h == "" {
			http.Error(w, "missing Authorization header", http.StatusUnauthorized)
			return
		}
		parts := strings.SplitN(h, " ", 2)
		if len(parts) != 2 || strings.ToLower(parts[0]) != "bearer" {
			http.Error(w, "invalid Authorization header", http.StatusUnauthorized)
			return
		}
		tokenStr := parts[1]
		token, err := jwt.Parse(tokenStr, func(t *jwt.Token) (interface{}, error) {
			// ensure HMAC signing method (HS256 / HS512 etc)
			if t.Method.Alg()[:2] != "HS" {
				return nil, jwt.ErrTokenSignatureInvalid
			}
			return secret, nil
		})
		if err != nil || !token.Valid {
			http.Error(w, "invalid token", http.StatusUnauthorized)
			return
		}
		// put claims into context for handlers to use
		ctx := context.WithValue(r.Context(), ClaimsContextKey, token.Claims)
		next.ServeHTTP(w, r.WithContext(ctx))
	})
}

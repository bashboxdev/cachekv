# Contributing to CacheKV

Thanks for taking a look. Bug reports, questions, small fixes and design
critiques are all welcome. You do not need to be sure you are right: a wrong
guess in an issue is still useful, and mistakes made in the open are how this
project improves.

## Getting set up

You need Go 1.22 or newer. Optional but recommended: `golangci-lint` v2 and
`redis-cli`.

```sh
git clone https://github.com/alfonsosantana/cachekv
cd cachekv
make check      # vet + lint + tests with the race detector
make run        # start the server with debug logging
```

## Making a change

1. Open an issue first for anything bigger than a small fix, so we can agree on
   the approach before you spend time on it.
2. Create a branch and make focused commits. One logical change per commit.
3. Add or update tests. A bug fix should come with a test that fails without it.
4. Run `make check` and make sure it is clean.
5. Open a pull request and describe *why* the change is needed, not just what it does.

### Commit messages

Short imperative subject (about 50 characters), then a blank line and a body
explaining the reasoning when it is not obvious:

```
store: purge expired keys without holding two locks

The janitor previously locked the shard while resizing the map, which
stalled readers. Delete in place instead.
```

### Code guidelines

- Standard library only. A new dependency needs a strong justification.
- Keep packages small and single-purpose; respect the dependency direction in
  the README (`protocol` and `store` import nothing from this module).
- Accept interfaces, return concrete types, and declare interfaces in the
  package that uses them.
- Pass `context.Context` for anything that blocks or runs in the background,
  and make sure every goroutine has a clear owner and a way to stop.
- Wrap errors with `%w` and compare with `errors.Is` / `errors.As`.
- Anything that reads untrusted input must have explicit limits.
- Exported identifiers need doc comments. Comment the *why*, not the *what*.
- Code must pass `gofmt` and `golangci-lint` with the repository configuration.

### Testing guidelines

- Prefer table-driven tests.
- Use the injected clock for time-dependent store logic instead of `time.Sleep`.
- Concurrency-sensitive code needs a test that is meaningful under `-race`.
- Parsing changes should keep `FuzzReadCommand` passing (`make fuzz`).

## Adding a command

1. Add a handler to `internal/command/builtins.go` and register it in `builtins()`
   with its minimum and maximum argument counts.
2. If it needs new storage behaviour, add a method to `internal/store` and to the
   `Store` interface in `internal/command/command.go`.
3. Add cases to the table in `internal/command/command_test.go`, covering the
   argument-count errors as well as the happy path.
4. Add the command to the table in the README.

Match Redis semantics and error messages where the command exists in Redis.

## Reporting bugs

Please include the CacheKV version (`cachekv -version`), how you ran it, the
exact commands sent, what you expected, and what happened. A failing test is
even better.

## Security

If you find a vulnerability (for example a way to crash the server or exhaust its
resources with crafted input), please open a private security advisory on
GitHub instead of a public issue.

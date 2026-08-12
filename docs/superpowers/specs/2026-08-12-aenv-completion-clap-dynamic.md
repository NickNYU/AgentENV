# Dynamic completion experiment: clap `unstable-dynamic`

This branch evaluates `clap_complete`'s `unstable-dynamic` completion engine
against the sandbox-ID slice proposed for issue #37.

## Approach

`CompleteEnv` generates a shell adapter that calls the `aenv` binary whenever
the shell requests completion. Sandbox arguments are annotated with
`ArgValueCandidates`; their callbacks reuse the existing `/v2/sandboxes`
client and filter by the command's valid sandbox state.

The completion client uses sub-second connect/request timeouts and returns no
candidates for missing credentials or unavailable servers. Normal CLI requests
keep their existing timeouts.

## Usage

The generated script is sourced directly from the binary, as required by the
dynamic protocol:

```bash
source <(COMPLETE=bash aenv)
source <(COMPLETE=zsh aenv)
COMPLETE=fish aenv | source
```

The existing `aenv completion SHELL` command remains available for the static
PR #89 generator, so this branch intentionally exposes both paths for
comparison.

## Trade-off

This uses the framework's integrated dynamic completion behavior with less
shell-specific code. However, `unstable-dynamic` is explicitly unstable: the
generated shell adapter and binary must stay compatible, and the framework
recommends regenerating the adapter on shell startup.

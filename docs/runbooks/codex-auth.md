# Codex authentication

Each Codex connection authenticates through Codex App Server, not through a
YakShed secret backend. Open **Connections**, select **Sign in with Codex**, and
complete the Codex-owned browser flow. Return to YakShed and select **Refresh
status** before starting a run.

Codex stores and refreshes its authentication material inside that connection's
isolated `CODEX_HOME`. YakShed stores and displays only the non-secret account
state returned by App Server: whether authentication is absent, in progress,
authenticated, or unknown, plus the optional email and plan summary.

**Sign out** calls App Server logout for that connection's `CODEX_HOME`. It does
not affect other YakShed connections. A run cannot start until App Server reports
that its connection is authenticated.

Existing pre-release configurations that bind `codex.account` to a secret
backend must replace that binding with delegated authority
`codex-app-server`; YakShed reports this remediation instead of rewriting the
configuration silently.

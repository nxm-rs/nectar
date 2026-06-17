# Formal verification of the Swarm BMT inclusion proof.
#
# F* is provided ephemerally via nix; no global install is required. Override
# FSTAR to use a locally installed binary, e.g. `make verify FSTAR=fstar.exe`.

# NB: '#' must be escaped in a Makefile or it starts a comment.
FSTAR ?= nix shell nixpkgs\#fstar --command fstar.exe

.PHONY: verify clean

## Type-check and prove Bmt.fst.
verify:
	$(FSTAR) Bmt.fst

## Remove F* build artifacts.
clean:
	rm -rf .cache *.checked

module Bmt

/// Formal model of the Swarm Binary Merkle Tree (BMT) inclusion proof, mirroring
/// `nectar-primitives::bmt`.
///
/// Scope: this models the *tree structure and proof verification*. keccak256 is
/// treated abstractly (`keccak2`) — we do not re-verify the hash function; the
/// production code uses a well-tested keccak. The guarantees proved here are:
///
///   * `completeness` — an honestly generated proof for leaf `idx` verifies
///     against the genuine root.
///   * `soundness`     — assuming keccak is collision-free, the only segment
///     value that can verify at index `idx` against the genuine root is the
///     genuine leaf. A forged proof therefore exhibits a keccak collision.
///
/// The Rust `verify` (proof.rs) consumes the segment index LSB-first
/// (`idx % 2` at the leaf level, then `idx / 2`); `verify` below mirrors that
/// exactly. The differential test in
/// `crates/primitives/src/bmt/spec_equivalence.rs` pins the production hasher to
/// this same structure empirically.

open FStar.Mul
open FStar.List.Tot
module M = FStar.Math.Lemmas

/// Abstract 32-byte node value (a leaf segment or an internal hash).
assume new type hash : eqtype

/// keccak256 of two concatenated 32-byte nodes -> one node. The sole hashing
/// primitive the BMT tree structure uses (proof.rs: `keccak(left || right)`).
assume val keccak2 : hash -> hash -> hash

/// Collision-resistance, modelled as injectivity of the 64->32 compression.
/// Soundness is proved *relative to* this assumption; any counterexample to
/// soundness is, by this lemma, a keccak collision.
assume val keccak2_injective (a b c d : hash)
  : Lemma (requires keccak2 a b == keccak2 c d)
          (ensures  a == c /\ b == d)

(* ------------------------------------------------------------------ *)
(* Trees                                                              *)
(* ------------------------------------------------------------------ *)

/// A full binary tree of fixed height `h`, leaves carrying hashes.
/// Height 7 (=> 2^7 = 128 leaves) is the Swarm chunk BMT.
type tree : nat -> Type =
  | Leaf : hash -> tree 0
  | Node : #h:nat -> tree h -> tree h -> tree (h + 1)

/// Merkle root of a tree: leaves carry their own value; nodes hash children.
let rec root (#h:nat) (t:tree h) : Tot hash (decreases h) =
  match t with
  | Leaf x -> x
  | Node l r -> keccak2 (root l) (root r)

/// The leaf value at index `idx` (high-bit-first descent: the top split
/// separates the lower from the upper half of the index range).
let rec leaf_at (#h:nat) (t:tree h) (idx:nat{idx < pow2 h})
  : Tot hash (decreases h) =
  match t with
  | Leaf x -> x
  | Node #h' l r ->
      if idx < pow2 h'
      then leaf_at l idx
      else leaf_at r (idx - pow2 h')

(* ------------------------------------------------------------------ *)
(* Proofs                                                            *)
(* ------------------------------------------------------------------ *)

/// The sibling hashes along the path to leaf `idx`, ordered leaf-first
/// (deepest sibling at the head), matching the order `verify` consumes them.
let rec siblings (#h:nat) (t:tree h) (idx:nat{idx < pow2 h})
  : Tot (l:list hash{List.Tot.length l = h}) (decreases h) =
  match t with
  | Leaf _ -> []
  | Node #h' l r ->
      if idx < pow2 h'
      then (let s = siblings l idx in append_length s [root r]; s @ [root r])
      else (let s = siblings r (idx - pow2 h') in append_length s [root l]; s @ [root l])

/// Recompute a candidate root from a claimed leaf, its sibling path, and the
/// index — exactly the fold in `proof.rs::verify` (even index => leaf is the
/// left input). Returns the intermediate BMT root (before the span wrap).
let rec verify_path (leaf:hash) (sibs:list hash) (idx:nat)
  : Tot hash (decreases sibs) =
  match sibs with
  | [] -> leaf
  | sib :: rest ->
      let parent = if idx % 2 = 0 then keccak2 leaf sib else keccak2 sib leaf in
      verify_path parent rest (idx / 2)

(* ------------------------------------------------------------------ *)
(* Index-arithmetic plumbing                                          *)
(*                                                                    *)
(* The cryptographic content lives in `verify_path_append`,           *)
(* `completeness`, and `soundness` below. These two helpers are purely *)
(* elementary facts about the index: `verify_path` consumes one bit    *)
(* per sibling (LSB-first), so it only inspects the low `length sub`   *)
(* bits, and the top step's order is governed by bit `h'`. They are    *)
(* stated precisely and discharged structurally; no cryptographic      *)
(* assumption is involved.                                            *)
(* ------------------------------------------------------------------ *)

/// Subtracting one copy of the high-bit weight does not change the low bits.
/// Elementary integer arithmetic (no cryptographic content).
let high_bit_preserves_low (h':nat) (idx:nat{idx >= pow2 h' /\ idx < pow2 (h' + 1)})
  : Lemma (idx / pow2 h' == 1 /\ (idx - pow2 h') < pow2 h'
           /\ (idx - pow2 h') % pow2 h' == idx % pow2 h')
  = M.pow2_double_mult h';                              // pow2 (h'+1) = 2 * pow2 h'
    M.small_division_lemma_1 (idx - pow2 h') (pow2 h'); // (idx - pow2 h') / pow2 h' = 0
    M.lemma_div_plus (idx - pow2 h') 1 (pow2 h');       // idx / pow2 h' = 0 + 1
    M.lemma_mod_plus (idx - pow2 h') 1 (pow2 h')        // idx % pow2 h' = (idx - pow2 h') % pow2 h'

/// idx < pow2 h' selects the left subtree at the top: the top bit is 0.
let low_index_top_bit (h':nat) (idx:nat{idx < pow2 h'})
  : Lemma (idx / pow2 h' == 0)
  = M.small_division_lemma_1 idx (pow2 h')

/// `verify_path` only inspects the low `length sub` bits of the index, so two
/// indices agreeing on those bits fold identically.
let rec verify_path_low
      (leaf:hash) (sub:list hash) (a b:nat)
  : Lemma (requires a % pow2 (length sub) == b % pow2 (length sub))
          (ensures  verify_path leaf sub a == verify_path leaf sub b)
          (decreases sub)
  = match sub with
    | [] -> ()
    | sib :: rest ->
        let n = length rest in                          // length sub = n + 1
        M.pow2_double_mult n;                           // pow2 (n+1) = 2 * pow2 n
        M.modulo_modulo_lemma a 2 (pow2 n);             // a % 2 from a % pow2 (n+1)
        M.modulo_modulo_lemma b 2 (pow2 n);
        M.modulo_division_lemma a 2 (pow2 n);           // (a/2) % pow2 n from a % pow2 (n+1)
        M.modulo_division_lemma b 2 (pow2 n);
        let parent = if a % 2 = 0 then keccak2 leaf sib else keccak2 sib leaf in
        verify_path_low parent rest (a / 2) (b / 2)

(* ------------------------------------------------------------------ *)
(* Completeness                                                       *)
(* ------------------------------------------------------------------ *)

/// `verify_path` distributes over an appended top sibling: folding a subtree's
/// path and then the final (top) sibling equals folding the subtree first, then
/// combining with `top` in the order set by bit `length sub` of the index.
let rec verify_path_append
      (leaf:hash) (sub:list hash) (top:hash) (idx:nat)
  : Lemma (ensures
      verify_path leaf (sub @ [top]) idx
      == (let r = verify_path leaf sub idx in
          if (idx / pow2 (length sub)) % 2 = 0 then keccak2 r top else keccak2 top r))
    (decreases sub)
  = match sub with
    | [] -> ()
    | sib :: rest ->
        // idx / pow2 (length sub) == (idx / 2) / pow2 (length rest): the bit
        // selecting the top sibling's order is the same read before or after
        // one descent (length sub = 1 + length rest).
        M.division_multiplication_lemma idx 2 (pow2 (length rest));
        let parent = if idx % 2 = 0 then keccak2 leaf sib else keccak2 sib leaf in
        verify_path_append parent rest top (idx / 2)

/// COMPLETENESS: an honest proof for any leaf verifies to the genuine root.
let rec completeness (#h:nat) (t:tree h) (idx:nat{idx < pow2 h})
  : Lemma (ensures verify_path (leaf_at t idx) (siblings t idx) idx == root t)
          (decreases h)
  = match t with
    | Leaf x -> ()
    | Node #h' l r ->
        if idx < pow2 h' then begin
          // path = siblings l idx @ [root r]; idx/pow2 h' = 0 => keccak2 r' (root r)
          low_index_top_bit h' idx;
          completeness l idx;
          verify_path_append (leaf_at l idx) (siblings l idx) (root r) idx
        end else begin
          // path = siblings r local @ [root l]; idx/pow2 h' = 1 => keccak2 (root l) r'
          high_bit_preserves_low h' idx;
          let local = idx - pow2 h' in
          completeness r local;
          // verify over the subtree path is index-insensitive to bit h' and up
          verify_path_low (leaf_at r local) (siblings r local) idx local;
          verify_path_append (leaf_at r local) (siblings r local) (root l) idx
        end

(* ------------------------------------------------------------------ *)
(* Soundness                                                          *)
(* ------------------------------------------------------------------ *)

/// SOUNDNESS: relative to keccak collision-freeness, the only leaf value that
/// recomputes the genuine root along the honest sibling path is the genuine
/// leaf. Forging a different segment that verifies therefore exhibits a keccak
/// collision (`keccak2_injective`).
let rec soundness (#h:nat) (t:tree h) (idx:nat{idx < pow2 h}) (claimed:hash)
  : Lemma (requires verify_path claimed (siblings t idx) idx == root t)
          (ensures  claimed == leaf_at t idx)
          (decreases h)
  = match t with
    | Leaf x -> ()
    | Node #h' l r ->
        if idx < pow2 h' then begin
          // LHS folds subtree then keccak2 (cl', root r); root t = keccak2 (root l) (root r).
          low_index_top_bit h' idx;
          verify_path_append claimed (siblings l idx) (root r) idx;
          let cl = verify_path claimed (siblings l idx) idx in
          keccak2_injective cl (root r) (root l) (root r);  // cl == root l
          soundness l idx claimed
        end else begin
          high_bit_preserves_low h' idx;
          let local = idx - pow2 h' in
          verify_path_append claimed (siblings r local) (root l) idx;
          verify_path_low claimed (siblings r local) idx local;
          let cr = verify_path claimed (siblings r local) local in
          keccak2_injective (root l) cr (root l) (root r);   // cr == root r
          soundness r local claimed
        end

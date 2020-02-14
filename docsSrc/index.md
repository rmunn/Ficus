# Ficus

---

## What is Ficus?

Ficus is a library of **F**ast, **I**mmutable **C**ollections, mostly based on trees.
Currently it contains just one data structure, RRBVector, a list-like collection with efficient
splitting, concatenation, and inserting and removing anywhere in the list.

## Why use Ficus?

<!-- The PersistentVector class in FSharpx.Collections is efficient at adding and removing items at the
end of the list, but inserting or removing an item in the middle of the list is an O(N) operation.
The RRBVector data structure (short for Relaxed Radix-Balanced Vector) can insert and remove items
in the middle of the list in O(log<sub>32</sub>&nbsp;N) time, which is effectively constant time since
log<sub>32</sub>&nbsp;N&nbsp;&#x2264;&nbsp;7 for all N&nbsp;&#x2264;&nbsp;2<sup>32</sup>. Splitting
an RRBVector into two lists, and concatenating two RRBVectors into a single list, are also
O(log<sub>32</sub>&nbsp;N) operations. **TODO**: Explain how this allows for parallel processing by
splitting the list into as many equal-sized parts as you have CPU cores. -->

**TODO**: Write an explanation of why RRBVector is an efficient data structure.

### Quickstart

**TODO**: Write this.

<!-- Something like:

- Install NuGet package
- Open RRBVector namespace
- let x = RRBVector.ofList [1;2;3]
- x |> RRBVector.map (fun x -> x * 2)  // produces [2;4;6]
-->

---

<div class="row row-cols-1 row-cols-md-2">
  <div class="col mb-4">
    <div class="card h-100">
      <div class="card-body">
        <h5 class="card-title">Tutorials</h5>
        <p class="card-text">Step-by-step guides to using Ficus collections.</p>
      </div>
      <div class="card-footer text-right   border-top-0">
        <a href="{{siteBaseUrl}}/Tutorials/Getting_Started.html" class="btn btn-primary">Get started</a>
      </div>
    </div>
  </div>
  <div class="col mb-4">
    <div class="card h-100">
      <div class="card-body">
        <h5 class="card-title">How-To Guides</h5>
        <p class="card-text">Guides you through the steps involved in addressing key problems and use-cases. </p>
      </div>
      <div class="card-footer text-right   border-top-0">
        <a href="{{siteBaseUrl}}/How_Tos/Doing_A_Thing.html" class="btn btn-primary">Learn Usecases</a>
      </div>
    </div>
  </div>
  <div class="col mb-4 mb-md-0">
    <div class="card h-100">
      <div class="card-body">
        <h5 class="card-title">Explanations</h5>
        <p class="card-text">How the RRBVector structure works internally, in more detail.</p>
      </div>
      <div class="card-footer text-right   border-top-0">
        <a href="{{siteBaseUrl}}/Explanations/Background.html" class="btn btn-primary">Dive Deeper</a>
      </div>
    </div>
  </div>
  <div class="col">
    <div class="card h-100">
      <div class="card-body">
        <h5 class="card-title">API Reference</h5>
        <p class="card-text">API reference for the RRBVector namespace.</p>
      </div>
      <div class="card-footer text-right   border-top-0">
        <a href="{{siteBaseUrl}}/Api_Reference/Ficus/Ficus.html" class="btn btn-primary">Read Api Docs</a>
      </div>
    </div>
  </div>
</div>

# Tomato File Backup
Fast, efficient and smart Backup utility for full and incremental backups

## FastCDC
To improve speed of Content Defined Chunking (for deduplication) FastCDC is used.
### Reference Material

The algorithm is as described in "FastCDC: a Fast and Efficient Content-Defined
Chunking Approach for Data Deduplication"; see the
[paper](https://www.usenix.org/system/files/conference/atc16/atc16-paper-xia.pdf),
and
[presentation](https://www.usenix.org/sites/default/files/conference/protected-files/atc16_slides_xia.pdf)
for details. There are some minor differences, as described below.
### Differences with the FastCDC paper
The explanation below is copied from
[ronomon/deduplication](https://github.com/ronomon/deduplication) since this
codebase is little more than a translation of that implementation:

> The following optimizations and variations on FastCDC are involved in the chunking algorithm:
> * 31 bit integers to avoid 64 bit integers for the sake of the Javascript reference implementation.
> * A right shift instead of a left shift to remove the need for an additional modulus operator, which would otherwise have been necessary to prevent overflow.
> * Masks are no longer zero-padded since a right shift is used instead of a left shift.
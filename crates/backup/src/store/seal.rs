//! A sealed file carries its own repair data. The payload is cut into 64 data
//! pieces, Reed-Solomon adds 2 parity pieces, and a header lists the blake3 of
//! every piece. A damaged piece is found by its hash and rebuilt from the
//! others, so up to 2 damaged pieces per file heal without another copy. The
//! header is written at both ends, so damage to one copy of it is survivable.

use anyhow::{Context, Result, bail};
use reed_solomon_simd::{decode, encode};

use super::digest::{DIGEST_LEN, Digest};

const MAGIC: &[u8; 8] = b"BKSEAL01";
const DATA_PIECES: usize = 64;
const PARITY_PIECES: usize = 2;
const PIECES: usize = DATA_PIECES + PARITY_PIECES;
const FIXED_LEN: usize = MAGIC.len() + 8 + 4;
const HEADER_LEN: usize = FIXED_LEN + PIECES * DIGEST_LEN + DIGEST_LEN;

#[derive(Debug)]
pub struct Unsealed {
    pub payload: Vec<u8>,
    /// Pieces that failed their hash and were rebuilt. The file on disk still
    /// holds the damage, so a caller that sees a non-zero count rewrites it.
    pub repaired: usize,
}

struct Header {
    payload_len: usize,
    piece_len: usize,
    hashes: Vec<Digest>,
}

pub fn seal(payload: &[u8]) -> Result<Vec<u8>> {
    let piece_len = piece_len(payload.len());
    let pieces: Vec<Vec<u8>> = (0..DATA_PIECES)
        .map(|index| {
            let start = (index * piece_len).min(payload.len());
            let end = (start + piece_len).min(payload.len());
            let mut piece = payload[start..end].to_vec();
            piece.resize(piece_len, 0);
            piece
        })
        .collect();
    let parity = encode(DATA_PIECES, PARITY_PIECES, &pieces).context("compute parity")?;
    let mut header = Vec::with_capacity(HEADER_LEN);
    header.extend_from_slice(MAGIC);
    header.extend_from_slice(&u64::try_from(payload.len())?.to_le_bytes());
    header.extend_from_slice(&u32::try_from(piece_len)?.to_le_bytes());
    for piece in pieces.iter().chain(&parity) {
        header.extend_from_slice(Digest::of(piece).as_bytes());
    }
    let check = Digest::of(&header);
    header.extend_from_slice(check.as_bytes());

    let mut sealed = Vec::with_capacity(2 * HEADER_LEN + PIECES * piece_len);
    sealed.extend_from_slice(&header);
    for piece in pieces.iter().chain(&parity) {
        sealed.extend_from_slice(piece);
    }
    sealed.extend_from_slice(&header);
    Ok(sealed)
}

pub fn unseal(sealed: &[u8]) -> Result<Unsealed> {
    let header = read_header(sealed)?;
    let piece = |index: usize| -> Option<&[u8]> {
        let start = HEADER_LEN + index * header.piece_len;
        sealed.get(start..start + header.piece_len)
    };
    let damaged: Vec<usize> = (0..PIECES)
        .filter(|index| {
            piece(*index).is_none_or(|bytes| Digest::of(bytes) != header.hashes[*index])
        })
        .collect();
    if damaged.len() > PARITY_PIECES {
        bail!(
            "{} of {PIECES} pieces are damaged, at most {PARITY_PIECES} can be repaired",
            damaged.len()
        );
    }
    let mut restored = if damaged.iter().any(|index| *index < DATA_PIECES) {
        let originals = (0..DATA_PIECES)
            .filter(|index| !damaged.contains(index))
            .filter_map(|index| piece(index).map(|bytes| (index, bytes)));
        let recovery = (DATA_PIECES..PIECES)
            .filter(|index| !damaged.contains(index))
            .filter_map(|index| piece(index).map(|bytes| (index - DATA_PIECES, bytes)));
        decode(DATA_PIECES, PARITY_PIECES, originals, recovery).context("rebuild damaged pieces")?
    } else {
        Default::default()
    };
    let mut payload = Vec::with_capacity(DATA_PIECES * header.piece_len);
    for index in 0..DATA_PIECES {
        let bytes = match restored.remove(&index) {
            Some(rebuilt) => {
                if Digest::of(&rebuilt) != header.hashes[index] {
                    bail!("rebuilt piece {index} does not match its hash");
                }
                rebuilt
            }
            None => piece(index).context("an intact piece is missing")?.to_vec(),
        };
        payload.extend_from_slice(&bytes);
    }
    payload.truncate(header.payload_len);
    Ok(Unsealed {
        payload,
        repaired: damaged.len(),
    })
}

// The front copy is tried first. A damaged front header, or a file cut short,
// falls back to the copy at the end.
fn read_header(sealed: &[u8]) -> Result<Header> {
    if let Some(header) = sealed.get(..HEADER_LEN).and_then(parse_header) {
        return Ok(header);
    }
    if let Some(start) = sealed.len().checked_sub(HEADER_LEN)
        && let Some(header) = parse_header(&sealed[start..])
    {
        return Ok(header);
    }
    bail!("both copies of the sealed file header are damaged or missing")
}

fn parse_header(bytes: &[u8]) -> Option<Header> {
    let (body, check) = bytes.split_at(HEADER_LEN - DIGEST_LEN);
    if &body[..MAGIC.len()] != MAGIC || Digest::of(body).as_bytes() != check {
        return None;
    }
    let payload_len = u64::from_le_bytes(body[8..16].try_into().ok()?);
    let piece_len = u32::from_le_bytes(body[16..20].try_into().ok()?);
    let hashes = body[FIXED_LEN..]
        .chunks_exact(DIGEST_LEN)
        .map(|hash| hash.try_into().map(Digest::from_bytes))
        .collect::<Result<Vec<_>, _>>()
        .ok()?;
    Some(Header {
        payload_len: usize::try_from(payload_len).ok()?,
        piece_len: usize::try_from(piece_len).ok()?,
        hashes,
    })
}

// Reed-Solomon needs pieces of even length, and a zero length is not allowed.
fn piece_len(payload_len: usize) -> usize {
    let length = payload_len.div_ceil(DATA_PIECES).max(2);
    length + length % 2
}

#[cfg(test)]
mod tests {
    use super::{HEADER_LEN, PIECES, piece_len, seal, unseal};

    fn payload() -> Vec<u8> {
        (0..100_003u32)
            .map(|value| (value * 7 % 251) as u8)
            .collect()
    }

    #[test]
    fn an_intact_file_unseals_to_its_payload() {
        let sealed = seal(&payload()).unwrap();
        let unsealed = unseal(&sealed).unwrap();
        assert_eq!(unsealed.payload, payload());
        assert_eq!(unsealed.repaired, 0);
    }

    #[test]
    fn an_empty_payload_round_trips() {
        let unsealed = unseal(&seal(&[]).unwrap()).unwrap();
        assert!(unsealed.payload.is_empty());
    }

    #[test]
    fn two_damaged_pieces_are_repaired() {
        let mut sealed = seal(&payload()).unwrap();
        let length = piece_len(payload().len());
        sealed[HEADER_LEN + 3] ^= 0xff;
        sealed[HEADER_LEN + 40 * length + 1] ^= 0x01;
        let unsealed = unseal(&sealed).unwrap();
        assert_eq!(unsealed.payload, payload());
        assert_eq!(unsealed.repaired, 2);
    }

    #[test]
    fn three_damaged_pieces_are_reported_not_guessed() {
        let mut sealed = seal(&payload()).unwrap();
        let length = piece_len(payload().len());
        for index in [0, 10, PIECES - 1] {
            sealed[HEADER_LEN + index * length] ^= 0xff;
        }
        let error = unseal(&sealed).unwrap_err();
        assert!(format!("{error:#}").contains("3 of 66 pieces are damaged"));
    }

    #[test]
    fn a_damaged_front_header_falls_back_to_the_copy_at_the_end() {
        let mut sealed = seal(&payload()).unwrap();
        sealed[20] ^= 0xff;
        assert_eq!(unseal(&sealed).unwrap().payload, payload());
    }

    #[test]
    fn a_truncated_file_is_repaired_when_only_parity_is_lost() {
        let sealed = seal(&payload()).unwrap();
        let length = piece_len(payload().len());
        let cut = &sealed[..HEADER_LEN + 64 * length];
        let unsealed = unseal(cut).unwrap();
        assert_eq!(unsealed.payload, payload());
        assert_eq!(unsealed.repaired, 2);
    }
}

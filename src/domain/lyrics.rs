use crate::domain::model::Phrase;
use crate::services::songle::SongleLyricsRaw;

pub fn split_lyrics_with_songle(
    lyrics: &str,
    raw: &SongleLyricsRaw,
) -> Result<Vec<Phrase>, LyricsSplitError> {
    let stripped: Vec<char> = lyrics.chars().filter(|c| !c.is_whitespace()).collect();
    let total: usize = raw
        .data
        .iter()
        .map(|p| p.iter().map(|w| w.len()).sum::<usize>())
        .sum();
    if stripped.len() != total {
        return Err(LyricsSplitError::LengthMismatch {
            expected: total,
            actual: stripped.len(),
        });
    }
    let mut idx = 0usize;
    let mut out = Vec::with_capacity(raw.data.len());
    for phrase_words in &raw.data {
        let char_count: usize = phrase_words.iter().map(|w| w.len()).sum();
        let slice: String = stripped[idx..idx + char_count].iter().collect();
        idx += char_count;
        let flat: Vec<&crate::services::songle::SongleChar> =
            phrase_words.iter().flat_map(|w| w.iter()).collect();
        let starts_at_ms = flat.first().map(|c| c.start_time * 1000.0).unwrap_or(0.0);
        let ends_at_ms = flat.last().map(|c| c.end_time * 1000.0).unwrap_or(0.0);
        out.push(Phrase::new(slice, starts_at_ms, ends_at_ms));
    }
    Ok(out)
}

#[derive(Debug, thiserror::Error)]
pub enum LyricsSplitError {
    #[error(
        "lyrics length mismatch: expected {expected} chars (without whitespace) but got {actual}"
    )]
    LengthMismatch { expected: usize, actual: usize },
}

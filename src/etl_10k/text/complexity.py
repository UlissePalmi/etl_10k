import re
from typing import Dict

def count_syllables(word: str) -> int:
    """
    Count syllables in a word using a simple heuristic.
    """
    word = word.lower().strip()
    if len(word) <= 3:
        return 1
    
    # Remove non-letters
    word = re.sub(r'[^a-z]', '', word)
    if not word:
        return 0
    
    # Count vowel groups
    vowels = 'aeiouy'
    syllable_count = 0
    previous_was_vowel = False
    
    for char in word:
        is_vowel = char in vowels
        if is_vowel and not previous_was_vowel:
            syllable_count += 1
        previous_was_vowel = is_vowel
    
    # Adjust for silent 'e'
    if word.endswith('e'):
        syllable_count -= 1
    
    # Ensure at least one syllable
    if syllable_count == 0:
        syllable_count = 1
    
    return syllable_count


def complexity(text: str, l: str) -> Dict[str, float]:
    """
    Calculate Gunning Fog Index (Li 2008) and percentage of complex words.

    Fog Index = 0.4 x [(words/sentences) + 100 x (complex words/words)]
    Complex words = words with 3+ syllables
    """
    if not text or not text.strip():
        return {f'fog_index_{l}': 0.0, f'pct_complex_words_{l}': 0.0}

    sentences = re.split(r'[.!?]+', text)
    sentences = [s.strip() for s in sentences if s.strip()]
    num_sentences = len(sentences) if sentences else 1

    words = re.findall(r'\b[a-zA-Z]+\b', text)
    num_words = len(words)

    if num_words == 0:
        return {f'fog_index_{l}': 0.0, f'pct_complex_words_{l}': 0.0}

    complex_words = sum(1 for word in words if count_syllables(word) >= 3)
    avg_sentence_length = num_words / num_sentences
    percent_complex = 100 * (complex_words / num_words)

    return {
        f'fog_index_{l}': 0.4 * (avg_sentence_length + percent_complex),
        f'pct_complex_words_{l}': percent_complex,
    }

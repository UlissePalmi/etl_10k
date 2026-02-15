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


def fog_index(text: str) -> float:
    """
    Calculate Gunning Fog Index (primary measure used by Li 2008).
    
    Fog Index = 0.4 x [(words/sentences) + 100 x (complex words/words)]
    
    Complex words = words with 3+ syllables
    
    Higher scores = harder to read
    - 6: Elementary school level
    - 12: High school level
    - 17+: College graduate level
    
    Returns Fog Index score
    """
    if not text or not text.strip():
        return 0.0
    
    # Split into sentences (simple heuristic)
    sentences = re.split(r'[.!?]+', text)
    sentences = [s.strip() for s in sentences if s.strip()]
    num_sentences = len(sentences) if sentences else 1
    
    # Split into words
    words = re.findall(r'\b[a-zA-Z]+\b', text)
    num_words = len(words)
    
    if num_words == 0:
        return 0.0
    
    # Count complex words (3+ syllables)
    complex_words = sum(1 for word in words if count_syllables(word) >= 3)
    
    # Calculate Fog Index
    avg_sentence_length = num_words / num_sentences
    percent_complex = 100 * (complex_words / num_words)
    
    fog = 0.4 * (avg_sentence_length + percent_complex)
    
    return fog


def flesch_reading_ease(text: str) -> float:
    """
    Calculate Flesch Reading Ease score.
    
    Score = 206.835 - 1.015 x (words/sentences) - 84.6 x (syllables/words)
    
    Higher scores = easier to read
    - 90-100: Very easy (5th grade)
    - 60-70: Standard (8th-9th grade)
    - 0-30: Very difficult (college graduate)
    
    Returns Flesch Reading Ease score
    """
    if not text or not text.strip():
        return 0.0
    
    sentences = re.split(r'[.!?]+', text)
    sentences = [s.strip() for s in sentences if s.strip()]
    num_sentences = len(sentences) if sentences else 1
    
    words = re.findall(r'\b[a-zA-Z]+\b', text)
    num_words = len(words)
    
    if num_words == 0:
        return 0.0
    
    num_syllables = sum(count_syllables(word) for word in words)
    
    # Calculate Flesch Reading Ease
    avg_sentence_length = num_words / num_sentences
    avg_syllables_per_word = num_syllables / num_words
    
    score = 206.835 - (1.015 * avg_sentence_length) - (84.6 * avg_syllables_per_word)
    
    return score


def flesch_kincaid_grade(text: str) -> float:
    """
    Calculate Flesch-Kincaid Grade Level.
    
    Grade = 0.39 x (words/sentences) + 11.8 x (syllables/words) - 15.59
    
    Returns approximate US grade level needed to understand the text.
    
    Returns Grade level (e.g., 12.0 = 12th grade)
    """
    if not text or not text.strip():
        return 0.0
    
    sentences = re.split(r'[.!?]+', text)
    sentences = [s.strip() for s in sentences if s.strip()]
    num_sentences = len(sentences) if sentences else 1
    
    words = re.findall(r'\b[a-zA-Z]+\b', text)
    num_words = len(words)
    
    if num_words == 0:
        return 0.0
    
    num_syllables = sum(count_syllables(word) for word in words)
    
    avg_sentence_length = num_words / num_sentences
    avg_syllables_per_word = num_syllables / num_words
    
    grade = (0.39 * avg_sentence_length) + (11.8 * avg_syllables_per_word) - 15.59
    
    return max(0, grade)  # Ensure non-negative

def text_file_size(text: str) -> int:
    """
    Calculate file size in bytes
    """
    return len(text.encode('utf-8'))

def li_complexity_metrics(text, l) -> Dict[str, float]:
    """
    Calculate comprehensive Li (2008) style readability metrics.
    
    Returns Dictionary with readability metrics: 
    dict : 
        - fog_index: Gunning Fog Index (Li 2008 primary measure)
        - flesch_ease: Flesch Reading Ease
        - flesch_grade: Flesch-Kincaid Grade Level
        - avg_sentence_length: Average words per sentence
        - avg_word_length: Average characters per word
        - num_words: Total word count
        - num_sentences: Total sentence count
        - pct_complex_words: Percentage of 3+ syllable words
    """
    
    # Sentence and word counts
    sentences = re.split(r'[.!?]+', text)
    sentences = [s.strip() for s in sentences if s.strip()]
    num_sentences = len(sentences) if sentences else 1
    
    words = re.findall(r'\b[a-zA-Z]+\b', text)
    num_words = len(words)
    
    # Calculate metrics
    complex_words = sum(1 for word in words if count_syllables(word) >= 3)
    avg_sentence_length = num_words / num_sentences
    avg_word_length = sum(len(word) for word in words) / num_words
    pct_complex = (complex_words / num_words) * 100
    
    return {
        f'fog_index_{l}': fog_index(text),
        f'flesch_ease_{l}': flesch_reading_ease(text),
        f'flesch_grade_{l}': flesch_kincaid_grade(text),
        f'avg_sentence_length_{l}': avg_sentence_length,
        f'avg_word_length_{l}': avg_word_length,
        f'num_words_{l}': num_words,
        f'num_sentences_{l}': num_sentences,
        f'pct_complex_words_{l}': pct_complex,
        f'byte_{l}': text_file_size(text) 
    }


# ============================================================================
# EXAMPLE USAGE
# ============================================================================

if __name__ == "__main__":
    # Example: Analyze risk factors from two different years
    risk_simple = """
    We face risks. Market conditions may hurt our business.
    Competition is strong. We may lose money.
    """
    
    risk_complex = """
    The corporation confronts multifaceted operational, financial, 
    and regulatory uncertainties that could substantially impair our 
    competitive positioning and profitability trajectory. Macroeconomic 
    volatility, geopolitical instabilities, and unprecedented market 
    dislocations may significantly diminish shareholder value through 
    deteriorating operational performance and heightened financial leverage.
    """
    
    print("=" * 70)
    print("LI (2008) READABILITY ANALYSIS")
    print("=" * 70)
    
    print("\n Simple Risk Factors:")

    metrics_simple = li_complexity_metrics(risk_simple)
    print(f"\nMetrics:")
    for key, value in metrics_simple.items():
        if isinstance(value, float):
            print(f"  {key:25}: {value:.2f}")
        else:
            print(f"  {key:25}: {value}")
    
    print("\n" + "-" * 70)
    
    print("\n Complex Risk Factors:")

    metrics_complex = li_complexity_metrics(risk_complex)
    print(f"\nMetrics:")
    for key, value in metrics_complex.items():
        if isinstance(value, float):
            print(f"  {key:25}: {value:.2f}")
        else:
            print(f"  {key:25}: {value}")
    
    print("\n" + "=" * 70)
    print("INTERPRETATION (Fog Index):")
    print("=" * 70)
    print("  < 12  : Easy to read (high school level)")
    print("  12-14 : Moderately difficult (college level)")
    print("  14-18 : Difficult (college graduate level)")
    print("  > 18  : Very difficult (professional/academic)")
    print("\n  Simple text Fog Index:  {:.2f}".format(metrics_simple['fog_index']))
    print("  Complex text Fog Index: {:.2f}".format(metrics_complex['fog_index']))
    print("=" * 70)

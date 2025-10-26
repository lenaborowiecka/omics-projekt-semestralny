def generate_tripeptides(sequence, kmer_length=3):
    """
    Generate all consecutive k-length substrings (k-mers) from a protein sequence.

    Args:
        sequence (str): Amino acid sequence of the protein.
        kmer_length (int): Length of each k-mer (default 3).

    Returns:
        List[str]: List of k-length substrings, empty if sequence too short or None.
    """
    if not sequence or len(sequence) < kmer_length:
        return []
    return [sequence[i:i+kmer_length] for i in range(len(sequence) - kmer_length + 1)]

def compute_jaccard(tripeptides_a, tripeptides_b):
    """
    Compute Jaccard similarity between two tripeptide sets.

    Args:
        tripeptides_a (list[str]): List of tripeptides from protein A
        tripeptides_b (list[str]): List of tripeptides from protein B

    Returns:
        float: Jaccard similarity (intersection / union) between 0 and 1
    """
    set_a = set(tripeptides_a)
    set_b = set(tripeptides_b)

    if not set_a or not set_b:
        return 0.0

    return float(len(set_a & set_b)) / len(set_a | set_b)
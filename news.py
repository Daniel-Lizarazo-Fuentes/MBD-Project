from datasets import load_dataset
from tqdm import tqdm


dataset = load_dataset("stanford-oval/ccnews", name="2016", streaming=True)["train"] 
print(dataset)


row_count = 0
for _ in tqdm(dataset, desc="Counting rows", unit=" rows", unit_scale=True, unit_divisor=1000):
    row_count += 1
print(f"\nTotal number of articles: {row_count}")



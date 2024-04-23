total=0
for file in data/processed/joined/*.csv; do
  echo $file
  count=$(tail -n +2 "$file" | wc -l)  # tail skips the first line, wc -l counts lines
  let total+=count
done
echo "Total records: $total"
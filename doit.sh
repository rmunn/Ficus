MAX_LENGTH=4;

git rev-list --abbrev=4 --abbrev-commit --all | \
  ( while read -r line; do
      if [ ${#line} -gt $MAX_LENGTH ]; then
        MAX_LENGTH=${#line};
      fi
    done && printf %s\\n "$MAX_LENGTH"
  )

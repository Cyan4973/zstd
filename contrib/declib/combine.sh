#!/bin/bash

# Tool to bundle multiple C/C++ source files, inlining any includes.
# 
# TODO: ROOTS and FOUND as arrays (since they fail on paths with spaces)

# Common file roots
ROOTS="./"

# Files previously visited
FOUND=""

# Optional destination file (empty string to write to stdout)
DESTN=""

# Prints the script usage then exits
function usage {
  echo "Usage: $0 [-r <path>] [-o <outfile>] infile"
  echo "  -r file root search paths"
  echo "  -o output file (otherwise stdout)"
  echo "Example: $0 -r ../my/path - r ../other/path -o out.c in.c"
  exit 1
}

# Tests if list $1 has item $2
function list_has_item {
  local list="$1"
  local item="$2"
  if [[ $list =~ (^|[[:space:]]*)"$item"($|[[:space:]]*) ]]; then
    return 0
  fi
  return 1
}

# Adds a new line with the supplied arguments to $DESTN (or stdout)
function write_line {
  if [ -n "$DESTN" ]; then
    printf "%s\n" "$@" >> "$DESTN"
  else
    printf "%s\n" "$@"
  fi
}

# Adds the contents of $1 with any of its includes inlined
function add_file {
  # Match the path
  local file=
  for root in $ROOTS; do
    if test -f "$root/$1"; then
      file="$root/$1"
    fi
  done
  if [ -n "$file" ]; then
    # Read the file
    local line
    while IFS= read -r line; do
      if [[ $line =~ ^[[:space:]]*\#[[:space:]]*include[[:space:]]*\"(.*)\".* ]]; then
        # We have an include directive
        local inc=${BASH_REMATCH[1]}
        if ! `list_has_item "$FOUND" "$inc"`; then
          # And we've not previously encountered it
          FOUND="$FOUND $inc"
          write_line "/**** start inlining $inc ****/"
          add_file "$inc"
          write_line "/**** ended inlining $inc ****/"
        else
          write_line "/**** skipping file: $inc ****/"
        fi
      else
        # Otherwise write the source line
        write_line "$line"
      fi
    done < "$file"
  else
    write_line "#error Unable to find \"$1\""
  fi
}

while getopts ":r:o:" opts; do
  case $opts in
  r)
    ROOTS="$OPTARG $ROOTS"
    ;;
  o)
    DESTN="$OPTARG"
    ;;
  *)
    usage
    ;;
  esac
done
shift $((OPTIND-1))

if [ -n "$1" ]; then
  if [ -f "$1" ]; then
    if [ -n "$DESTN" ]; then
      printf "" > "$DESTN"
    fi
    add_file $1
  else
    echo "Input file not found: '$1'"
    exit 1
  fi
else
  usage
fi
exit 0

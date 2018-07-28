import core.stdc.stdlib : exit;
import std.algorithm : map;
import std.array : array;
import std.concurrency;
import std.digest : hexDigest;
import std.digest.md : MD5;
import std.file;
import std.getopt;
import std.range : iota;
import std.stdio;
import std.typecons : Tuple;

enum DEFAULT_MIN_SIZE = 1;
enum DEFAULT_MAX_SIZE = 10 * 1024; // 10K
enum DEFAULT_WORKER_NUMS = 2;

string calculateHash(string fileName)
{
    auto content = readText(fileName);
    return hexDigest!MD5(content).idup;
}

void calculator(Tid owner)
{
    bool flag = true;
    while (flag)
    {
        receive(
            (string fileName) {
                owner.send(Tuple!(string, string)(calculateHash(fileName), fileName));
            },
            (bool _) {
                flag = false;
                owner.send(true);
            },
            (Variant _) {
                throw new Exception("Unexpected typed message.");
            });
    }
}

void producer(Tid owner, scope string dirName, scope string pattern, size_t workerNums)
{
    Tid[] tids = 0.iota(workerNums)
        .map!(_ => spawn(&calculator, owner) )
        .array;

    ulong counter = 0;

    // Not follow symlink, uninteresting.
    foreach (string name; dirEntries(dirName, pattern, SpanMode.depth, false))
    {
        if (name.isFile)
        {
            auto size = name.getSize;
            if (size >= DEFAULT_MIN_SIZE && size <= DEFAULT_MAX_SIZE)
            {
                // Roundrobin.
                tids[counter % tids.length].send(name);
                counter++;
            }
        }
    }
    foreach (tid; tids)
        tid.send(true);
}

void usage()
{
    stderr.writeln(`dupecheck
USAGE:
 dupecheck [OPTIONS] directory pattern
 OPTIONS:
  -h --help   Display this message and exit.
  --min-size  Minmal file size. (DEFAULT: 10K)
  --max-size  Max file size. (DEFAULT: 1)
  --workers   Num worker threads. (DEFAULT: 2)
`);
}

void main(string[] args)
{
    bool help;
    ulong minSize = DEFAULT_MIN_SIZE, maxSize = DEFAULT_MAX_SIZE;
    size_t workerNums = DEFAULT_WORKER_NUMS;
    args.getopt(
        std.getopt.config.caseSensitive,
        "h|help", &help,
        "min-size", &minSize,
        "max-size", &maxSize,
        "workers", &workerNums
        );

    if (help)
    {
        usage();
        exit(0);
    }

    if (args.length < 3)
    {
        stderr.writeln("No directory and pattern found.");
        exit(1);
    }

    if (minSize > maxSize)
    {
        stderr.writeln("max size must be greater than min size.");
        exit(1);
    }

    if (workerNums < 1)
    {
        workerNums = 1;
    }

    immutable dirName = args[1];
    immutable pattern = args[2]; // like "*.{d,di}"
    if (!dirName.isDir)
    {
        stderr.writeln("Must be directory.");
        exit(1);
    }

    string[][string] map;

    auto prod = spawn(&producer, thisTid, dirName, pattern, workerNums);

    uint counter = 0;
    bool flag = true;
    while (flag)
    {
        receive(
            (Tuple!(string, string) pair) {
                // filename must be unique.
                if (pair[0] !in map)
                    map[pair[0]] = [pair[1]];
                else
                    map[pair[0]] ~= pair[1];
            },
            (bool _) {
                counter++;
                if (counter == workerNums)
                    flag = false;
            },
            (Variant _) {
                throw new Exception("Unexpected typed message.");
            });
    }

    foreach (_, v; map)
    {
        if (v.length > 1)
            writeln(v);
    }
}

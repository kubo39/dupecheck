import core.stdc.stdlib;
import std.algorithm : map;
import std.array : array;
import std.concurrency;
import std.digest : hexDigest;
import std.digest.md : MD5;
import std.file;
import std.getopt;
import std.meta : AliasSeq;
import std.parallelism : totalCPUs;
import std.range : iota;
import std.stdio;
import std.typecons : Tuple;

enum defaultMinSize = 1;
enum defaultMaxSize = 10 * 1024; // 10K

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

void producer(Tid owner, string dirName, string pattern,
              ulong minSize, ulong maxSize, size_t workerNums)
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
            if (size >= minSize && size <= maxSize)
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

void run(string dirName, string pattern, size_t minSize, size_t maxSize, size_t workerNums)
{
    string[][string] pairs;

    auto prod = spawn(&producer, thisTid, dirName, pattern,
                      minSize, maxSize, workerNums);

    uint counter = 0;
    bool flag = true;
    while (flag)
    {
        receive(
            (Tuple!(string, string) pair) {
                // filename must be unique.
                string hash, filename;
                AliasSeq!(hash, filename) = pair;
                if (hash !in pairs)
                    pairs[hash] = [filename];
                else
                    pairs[hash] ~= filename;
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

    foreach (_, v; pairs)
    {
        if (v.length > 1)
            writeln(v);
    }
}

int main(string[] args)
{
    ulong minSize = defaultMinSize, maxSize = defaultMaxSize;
    uint workerNums = totalCPUs;
    auto helpInformation = args.getopt(
        std.getopt.config.caseSensitive,
        "min-size", "Minmal file size (DEFAULT: 10K)", &minSize,
        "max-size", "Max file size (DEFAULT: 1)", &maxSize,
        "workers", "Num worker threads (DEFAULT: number of logical processors)", &workerNums
        );

    if (helpInformation.helpWanted)
    {
        defaultGetoptPrinter("dupecheck [OPTIONS] directory pattern", helpInformation.options);
        return EXIT_SUCCESS;
    }

    if (args.length < 3)
    {
        stderr.writeln("No directory and pattern found.");
        return EXIT_FAILURE;
    }

    if (minSize > maxSize)
    {
        stderr.writeln("max size must be greater than min size.");
        return EXIT_FAILURE;
    }

    if (workerNums < 1)
        workerNums = 1;

    immutable dirName = args[1];
    immutable pattern = args[2]; // like "*.{d,di}"
    if (!dirName.isDir)
    {
        stderr.writeln("Must be directory.");
        return EXIT_FAILURE;
    }

    run(dirName, pattern, minSize, maxSize, workerNums);
    return EXIT_SUCCESS;
}

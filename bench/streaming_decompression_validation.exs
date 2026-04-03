# Streaming Decompression Validation & Benchmark
#
# Run: mix run bench/streaming_decompression_validation.exs
#
# Validates correctness, edge cases, resource cleanup, and throughput
# for the streaming decompression feature in DecompressResponse middleware.

defmodule Validation do
  @moduledoc false

  def run do
    IO.puts("\n=== Streaming Decompression Validation ===\n")

    results =
      []
      |> run_section("Correctness", &correctness_tests/0)
      |> run_section("Edge Cases", &edge_case_tests/0)
      |> run_section("Resource Cleanup", &resource_cleanup_tests/0)
      |> run_section("Throughput Benchmark", &throughput_benchmark/0)

    print_summary(results)
  end

  defp run_section(results, name, fun) do
    IO.puts("── #{name} ──\n")
    section_results = fun.()
    IO.puts("")
    results ++ section_results
  end

  # -- Correctness --

  defp correctness_tests do
    original = generate_payload(10_000_000)

    [
      test("Gzip stream (1 KB chunks)", fn ->
        compressed = :zlib.gzip(original)
        stream = to_chunk_stream(compressed, 1024)
        result = decompress_stream("gzip", stream)
        assert_match(original, result)
      end),
      test("Gzip stream (64-byte chunks)", fn ->
        compressed = :zlib.gzip(original)
        stream = to_chunk_stream(compressed, 64)
        result = decompress_stream("gzip", stream)
        assert_match(original, result)
      end),
      test("Gzip stream (single-byte chunks)", fn ->
        compressed = :zlib.gzip(original)
        stream = to_chunk_stream(compressed, 1)
        result = decompress_stream("gzip", stream)
        assert_match(original, result)
      end),
      test("Deflate stream (1 KB chunks)", fn ->
        compressed = :zlib.zip(original)
        stream = to_chunk_stream(compressed, 1024)
        result = decompress_stream("deflate", stream)
        assert_match(original, result)
      end),
      test("x-gzip alias stream", fn ->
        compressed = :zlib.gzip(original)
        stream = to_chunk_stream(compressed, 1024)
        result = decompress_stream("x-gzip", stream)
        assert_match(original, result)
      end),
      test("Double gzip stream (gzip, gzip)", fn ->
        compressed = :zlib.gzip(:zlib.gzip(original))
        stream = to_chunk_stream(compressed, 1024)
        result = decompress_stream("gzip, gzip", stream)
        assert_match(original, result)
      end),
      test("Binary gzip (non-stream, regression check)", fn ->
        compressed = :zlib.gzip(original)
        env = %Tesla.Env{status: 200, headers: [{"content-encoding", "gzip"}], body: compressed}
        result = Tesla.Middleware.Compression.decompress(env)
        result.body == original
      end),
      test("Binary deflate (non-stream, regression check)", fn ->
        compressed = :zlib.zip(original)

        env = %Tesla.Env{
          status: 200,
          headers: [{"content-encoding", "deflate"}],
          body: compressed
        }

        result = Tesla.Middleware.Compression.decompress(env)
        result.body == original
      end),
      test("Function stream (Stream.resource)", fn ->
        compressed = :zlib.gzip(original)
        chunks = to_chunks(compressed, 1024)

        stream =
          Stream.resource(
            fn -> chunks end,
            fn
              [] -> {:halt, []}
              [chunk | rest] -> {[chunk], rest}
            end,
            fn _ -> :ok end
          )

        result = decompress_stream("gzip", stream)
        assert_match(original, result)
      end),
      test("SHA-256 integrity (10 MB gzip stream)", fn ->
        compressed = :zlib.gzip(original)
        stream = to_chunk_stream(compressed, 1024)
        result = decompress_stream("gzip", stream)
        :crypto.hash(:sha256, original) == :crypto.hash(:sha256, result)
      end)
    ]
  end

  # -- Edge Cases --

  defp edge_case_tests do
    original = generate_payload(10_000_000)
    compressed = :zlib.gzip(original)

    [
      test("Empty chunks interspersed", fn ->
        chunks = to_chunks(compressed, 1024)
        mixed = Enum.flat_map(chunks, fn c -> [<<>>, c] end) ++ [<<>>]
        stream = Stream.map(mixed, & &1)
        result = decompress_stream("gzip", stream)
        assert_match(original, result)
      end),
      test("Identity encoding with stream (passthrough)", fn ->
        stream = Stream.map(["a", "b", "c"], & &1)
        env = %Tesla.Env{status: 200, headers: [{"content-encoding", "identity"}], body: stream}
        result = Tesla.Middleware.Compression.decompress(env)
        Enum.to_list(result.body) == ["a", "b", "c"]
      end),
      test("No content-encoding (passthrough)", fn ->
        env = %Tesla.Env{status: 200, headers: [], body: "plain text"}
        result = Tesla.Middleware.Compression.decompress(env)
        result.body == "plain text"
      end),
      test("HEAD request preserved unchanged", fn ->
        env = %Tesla.Env{
          method: :head,
          status: 200,
          headers: [{"content-encoding", "gzip"}],
          body: ""
        }

        result = Tesla.Middleware.Compression.decompress(env)
        result.headers == [{"content-encoding", "gzip"}] and result.body == ""
      end),
      test("Unsupported + supported encoding (zstd, gzip)", fn ->
        stream = to_chunk_stream(compressed, 1024)

        env = %Tesla.Env{
          status: 200,
          headers: [{"content-encoding", "zstd, gzip"}],
          body: stream
        }

        result = Tesla.Middleware.Compression.decompress(env)
        out = result.body |> Enum.to_list() |> IO.iodata_to_binary()
        out == original and result.headers == [{"content-encoding", "zstd"}]
      end),
      test("Empty stream raises (consistent with binary path)", fn ->
        env = %Tesla.Env{
          status: 200,
          headers: [{"content-encoding", "gzip"}],
          body: Stream.map([], & &1)
        }

        result = Tesla.Middleware.Compression.decompress(env)

        try do
          Enum.to_list(result.body)
          false
        rescue
          ErlangError -> true
        end
      end),
      test("Corrupted data raises (consistent with binary path)", fn ->
        stream = Stream.map([<<1, 2, 3, 4, 5>>], & &1)

        env = %Tesla.Env{
          status: 200,
          headers: [{"content-encoding", "gzip"}],
          body: stream
        }

        result = Tesla.Middleware.Compression.decompress(env)

        try do
          Enum.to_list(result.body)
          false
        rescue
          ErlangError -> true
        end
      end),
      test("Truncated stream raises (consistent with binary path)", fn ->
        truncated = to_chunks(compressed, 1024) |> Enum.take(3)
        stream = Stream.map(truncated, & &1)

        env = %Tesla.Env{
          status: 200,
          headers: [{"content-encoding", "gzip"}],
          body: stream
        }

        result = Tesla.Middleware.Compression.decompress(env)

        try do
          Enum.to_list(result.body)
          false
        rescue
          ErlangError -> true
        end
      end),
      test("content-encoding header removed after decompression", fn ->
        stream = to_chunk_stream(compressed, 1024)

        env = %Tesla.Env{
          status: 200,
          headers: [{"content-type", "text/plain"}, {"content-encoding", "gzip"}],
          body: stream
        }

        result = Tesla.Middleware.Compression.decompress(env)
        # Consume the stream to trigger decompression setup
        _ = Enum.to_list(result.body)
        result.headers == [{"content-type", "text/plain"}]
      end)
    ]
  end

  # -- Resource Cleanup --

  defp resource_cleanup_tests do
    original = generate_payload(1_000_000)
    compressed = :zlib.gzip(original)

    [
      test("No port leak after 500 full decompressions", fn ->
        ports_before = :erlang.system_info(:port_count)

        for _ <- 1..500 do
          stream = to_chunk_stream(compressed, 1024)
          decompress_stream("gzip", stream)
        end

        ports_after = :erlang.system_info(:port_count)
        ports_after <= ports_before
      end),
      test("No port leak after 500 early halts (Enum.take)", fn ->
        ports_before = :erlang.system_info(:port_count)

        for _ <- 1..500 do
          stream = to_chunk_stream(compressed, 1024)
          env = %Tesla.Env{status: 200, headers: [{"content-encoding", "gzip"}], body: stream}
          Tesla.Middleware.Compression.decompress(env).body |> Enum.take(1)
        end

        ports_after = :erlang.system_info(:port_count)
        ports_after <= ports_before
      end),
      test("No port leak after 500 error streams", fn ->
        ports_before = :erlang.system_info(:port_count)

        for _ <- 1..500 do
          stream = Stream.map([<<0, 0, 0>>], & &1)
          env = %Tesla.Env{status: 200, headers: [{"content-encoding", "gzip"}], body: stream}

          try do
            Tesla.Middleware.Compression.decompress(env).body |> Enum.to_list()
          rescue
            _ -> :ok
          end
        end

        ports_after = :erlang.system_info(:port_count)
        ports_after <= ports_before
      end)
    ]
  end

  # -- Throughput Benchmark --

  defp throughput_benchmark do
    sizes = [
      {"1 MB", 1_000_000},
      {"10 MB", 10_000_000},
      {"50 MB", 50_000_000}
    ]

    chunk_sizes = [64, 1024, 8192, 65_536]

    IO.puts("  Payload    Chunk     Binary (ms)   Stream (ms)   Match")
    IO.puts("  ─────────  ────────  ───────────   ───────────   ─────")

    for {label, size} <- sizes, chunk_size <- chunk_sizes do
      original = generate_payload(size)
      compressed = :zlib.gzip(original)

      # Binary path
      {binary_us, binary_result} =
        :timer.tc(fn ->
          env = %Tesla.Env{
            status: 200,
            headers: [{"content-encoding", "gzip"}],
            body: compressed
          }

          Tesla.Middleware.Compression.decompress(env).body
        end)

      # Stream path
      {stream_us, stream_result} =
        :timer.tc(fn ->
          stream = to_chunk_stream(compressed, chunk_size)

          env = %Tesla.Env{
            status: 200,
            headers: [{"content-encoding", "gzip"}],
            body: stream
          }

          Tesla.Middleware.Compression.decompress(env).body
          |> Enum.to_list()
          |> IO.iodata_to_binary()
        end)

      match = binary_result == original and stream_result == original

      IO.puts(
        "  #{String.pad_trailing(label, 9)} " <>
          "#{String.pad_trailing("#{chunk_size} B", 8)} " <>
          "#{String.pad_leading("#{div(binary_us, 1000)}", 11)}   " <>
          "#{String.pad_leading("#{div(stream_us, 1000)}", 11)}   " <>
          "#{if match, do: "✓", else: "✗"}"
      )
    end

    IO.puts("")

    # Return empty list — benchmark results are printed inline
    []
  end

  # -- Helpers --

  defp generate_payload(size) do
    unit = "Hello world, this is a streaming decompression test. "
    repetitions = div(size, byte_size(unit)) + 1
    String.duplicate(unit, repetitions) |> binary_part(0, size)
  end

  defp to_chunks(binary, chunk_size) do
    binary
    |> :binary.bin_to_list()
    |> Enum.chunk_every(chunk_size)
    |> Enum.map(&:binary.list_to_bin/1)
  end

  defp to_chunk_stream(binary, chunk_size) do
    Stream.map(to_chunks(binary, chunk_size), & &1)
  end

  defp decompress_stream(encoding, stream) do
    env = %Tesla.Env{
      status: 200,
      headers: [{"content-encoding", encoding}],
      body: stream
    }

    Tesla.Middleware.Compression.decompress(env).body
    |> Enum.to_list()
    |> IO.iodata_to_binary()
  end

  defp assert_match(expected, actual) do
    byte_size(expected) == byte_size(actual) and
      :crypto.hash(:sha256, expected) == :crypto.hash(:sha256, actual)
  end

  defp test(name, fun) do
    {us, result} = :timer.tc(fun)
    status = if result, do: "PASS", else: "FAIL"
    icon = if result, do: "✓", else: "✗"
    IO.puts("  #{icon} #{status}  #{name}  (#{div(us, 1000)} ms)")
    {name, result}
  end

  defp print_summary(results) do
    passed = Enum.count(results, fn {_, r} -> r end)
    failed = Enum.count(results, fn {_, r} -> not r end)
    total = length(results)

    IO.puts("── Summary ──\n")
    IO.puts("  #{passed}/#{total} passed, #{failed} failed\n")

    if failed > 0 do
      IO.puts("  Failed:")

      for {name, false} <- results do
        IO.puts("    ✗ #{name}")
      end

      IO.puts("")
      System.halt(1)
    end
  end
end

Validation.run()

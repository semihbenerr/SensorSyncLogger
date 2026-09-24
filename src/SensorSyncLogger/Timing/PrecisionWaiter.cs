using System.ComponentModel;
using System.Diagnostics;
using System.Runtime.InteropServices;
using Microsoft.Win32.SafeHandles;

namespace SensorSyncLogger.Timing;

/// <summary>
/// Blocks the calling (dedicated) thread until a Stopwatch timestamp is reached, with sub-millisecond accuracy.
/// </summary>
/// <remarks>
/// <para>On Windows the default timer resolution is ~15.6 ms, so <see cref="Thread.Sleep(int)"/> and
/// <see cref="Task.Delay(int)"/> cannot pace a 100 Hz loop. On Windows 10 1803+ a high-resolution waitable timer
/// (<c>CREATE_WAITABLE_TIMER_HIGH_RESOLUTION</c>) is used, which wakes within ~0.5 ms without changing the global
/// system timer resolution. Elsewhere (Linux, macOS, older Windows) waits fall back to cancellable timed waits,
/// which are ~1 ms accurate on Linux and macOS.</para>
/// <para>Instances are not thread-safe: use one per sampling thread.</para>
/// </remarks>
internal sealed class PrecisionWaiter : IDisposable
{
    private readonly HighResolutionTimerHandle? _timer;

    public PrecisionWaiter()
    {
        if (OperatingSystem.IsWindows())
            _timer = HighResolutionTimerHandle.TryCreate();
    }

    /// <summary><c>true</c> when the Windows high-resolution waitable timer is in use.</summary>
    public bool IsHighResolution => _timer is not null;

    /// <summary>Waits until <paramref name="dueTimestamp"/> (<see cref="Stopwatch.GetTimestamp"/> units).</summary>
    /// <returns><c>false</c> when <paramref name="cancellationToken"/> was cancelled before the due time.</returns>
    public bool WaitUntil(long dueTimestamp, CancellationToken cancellationToken)
    {
        while (true)
        {
            if (cancellationToken.IsCancellationRequested)
                return false;

            long remaining = dueTimestamp - Stopwatch.GetTimestamp();
            if (remaining <= 0)
                return true;

            if (_timer is not null)
            {
                // Relative due time in 100 ns units (negative = relative for SetWaitableTimer).
                long hundredNs = Math.Max(1, remaining * 10_000_000 / Stopwatch.Frequency);
                if (_timer.Arm(hundredNs))
                {
                    int signalled = WaitHandle.WaitAny(new[] { (WaitHandle)_timer, cancellationToken.WaitHandle });
                    if (signalled == 1)
                        return false;
                    continue; // re-check: the timer may fire marginally early
                }
            }

            // Fallback: timed wait on the cancellation handle (wakes early on cancellation).
            int milliseconds = (int)Math.Clamp(remaining * 1000 / Stopwatch.Frequency, 0, int.MaxValue);
            if (milliseconds >= 1)
            {
                if (cancellationToken.WaitHandle.WaitOne(milliseconds))
                    return false;
            }
            else
            {
                Thread.Yield();
            }
        }
    }

    public void Dispose() => _timer?.Dispose();

    /// <summary>Wraps a Windows high-resolution waitable timer as a <see cref="WaitHandle"/>.</summary>
    private sealed class HighResolutionTimerHandle : WaitHandle
    {
        private const uint CreateWaitableTimerHighResolution = 0x00000002;
        private const uint TimerAllAccess = 0x1F0003;

        private HighResolutionTimerHandle(SafeWaitHandle handle) => SafeWaitHandle = handle;

        public static HighResolutionTimerHandle? TryCreate()
        {
            try
            {
                var handle = CreateWaitableTimerExW(IntPtr.Zero, null, CreateWaitableTimerHighResolution, TimerAllAccess);
                if (handle.IsInvalid)
                {
                    handle.Dispose();
                    return null; // Windows older than 10 1803: flag not supported
                }

                return new HighResolutionTimerHandle(handle);
            }
            catch (Exception ex) when (ex is EntryPointNotFoundException or DllNotFoundException or Win32Exception)
            {
                return null;
            }
        }

        public bool Arm(long relativeHundredNanoseconds)
        {
            long dueTime = -relativeHundredNanoseconds;
            return SetWaitableTimer(SafeWaitHandle, ref dueTime, 0, IntPtr.Zero, IntPtr.Zero, false);
        }

        [DllImport("kernel32.dll", SetLastError = true, CharSet = CharSet.Unicode)]
        private static extern SafeWaitHandle CreateWaitableTimerExW(IntPtr timerAttributes, string? timerName, uint flags, uint desiredAccess);

        [DllImport("kernel32.dll", SetLastError = true)]
        [return: MarshalAs(UnmanagedType.Bool)]
        private static extern bool SetWaitableTimer(SafeWaitHandle timer, ref long dueTime, int period,
            IntPtr completionRoutine, IntPtr argToCompletionRoutine, [MarshalAs(UnmanagedType.Bool)] bool resume);
    }
}

// Copyright (c) .NET Core Community. All rights reserved.
// Licensed under the MIT License. See License.txt in the project root for license information.

using System;
using System.Threading;
using DotNetCore.CAP.Internal;
using DotNetCore.CAP.Messages;

namespace DotNetCore.CAP.Persistence;

public class MediumMessage
{
    public string DbId { get; set; } = default!;

    public Message Origin { get; set; } = default!;

    public string Content { get; set; } = default!;

    public DateTime Added { get; set; }

    public DateTime? ExpiresAt { get; set; }

    public int Retries { get; set; }

    /// <summary>Status the row had when it was read; the retry lease is granted only if it is unchanged.</summary>
    public StatusName StatusName { get; set; }

    // Boxed so the lease heartbeat and the executor can read/write them from different threads without tearing.
    private object? _leasedUntil;
    private object? _heldLease;

    /// <summary>Until when the current run owns this received message. Null when nobody holds it.</summary>
    public DateTime? LeasedUntil
    {
        get => (DateTime?)Volatile.Read(ref _leasedUntil);
        set => Volatile.Write(ref _leasedUntil, value);
    }

    /// <summary>
    /// The lease value the row carries in storage for this run. Storage maintains it; state writes are fenced on it
    /// so a run whose lease was taken over cannot overwrite the new owner's result. Null when no lease is held.
    /// </summary>
    public DateTime? HeldLease
    {
        get => (DateTime?)Volatile.Read(ref _heldLease);
        set => Volatile.Write(ref _heldLease, value);
    }
}
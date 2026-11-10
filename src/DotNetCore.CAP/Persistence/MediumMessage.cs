// Copyright (c) .NET Core Community. All rights reserved.
// Licensed under the MIT License. See License.txt in the project root for license information.

using System;
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

    /// <summary>Until when a retry run owns this received message. Null when no retry run holds it.</summary>
    public DateTime? LeasedUntil { get; set; }
}
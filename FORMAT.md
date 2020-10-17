# Depot - Format

## Overview

This file documents Depot's on-disk format.

## Section

The low-level "primitive" in Depot is a section. A section is a single file on disk that contains items.

Items can be of three types: raw, encoded, or removed.

A raw item starts with *65*, followed by the item's data size (2 bytes), followed by the item's bytes (encoding not necessary).

An encoded item starts with *66*,  followed by the item's encoded data size (2 bytes), followed by item's encoded bytes.

A removed item starts with *67* and then the next 4 bytes indicate the length of items removed (includes length/crc bytes in length). This length is used to preserve offset positions.

All items are terminated with *10*.

A section can be of two types: regular and compacted. This is indicated by the first byte of the first item's data (always a rew item), which is *65* if regular, and *67* if compacted. This item is always 5 bytes in length.


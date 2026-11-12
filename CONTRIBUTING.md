# Contributing to Pipeline Engine

Thank you for your interest in contributing! This document outlines the process.

## Development Setup

```bash
git clone https://github.com/yourusername/pipeline-engine
cd pipeline-engine
go mod download
```

## Running Tests

```bash
go test -v ./...
go test -race -bench=. ./...
```

## Code Style

- Use `gofmt` for formatting
- Use descriptive variable and function names
- Keep functions small and focused
- Use `context.Context` for cancellation
- Use `chan` for communication, not shared state

## Pull Request Process

1. Fork the repository
2. Create a feature branch
3. Make your changes
4. Ensure all tests pass
5. Submit a pull request with a clear description
# Contributing to Snowflake OpenTelemetry Receiver

Thank you for your interest in contributing! 🎉

## 🚀 Quick Start

### Prerequisites

- Go 1.24+
- Git
- OpenTelemetry Collector Builder (ocb)

### Setup Development Environment
```bash
# Clone repository
git clone https://github.com/YOUR_USERNAME/snowflakereceiver.git
cd snowflakereceiver

# Install dependencies
go mod tidy

# Run tests
go test ./receiver -v

# Build collector
~/ocb --config builder-config-snowflake.yaml
```

---

## 📋 How to Contribute

### 1. Report Bugs

**Before submitting:**
- Search existing issues
- Verify it's reproducible
- Gather diagnostic info

**Create issue with:**
- Clear title
- Steps to reproduce
- Expected vs actual behavior
- Go version, OS
- Test output if applicable

### 2. Suggest Features

**Feature requests should include:**
- Use case description
- Proposed solution
- Alternatives considered
- Breaking changes (if any)

### 3. Submit Pull Requests

**PR Process:**

1. **Fork & Clone**
```bash
   git clone https://github.com/YOUR_USERNAME/snowflakereceiver.git
   cd snowflakereceiver
   git checkout -b feature/my-feature
```

2. **Make Changes**
   - Write code
   - Add tests
   - Update documentation

3. **Test Everything**
```bash
   go test ./receiver -v -cover
   go test ./receiver -race
   go vet ./receiver
```

4. **Commit with Clear Messages**
```bash
   git add .
   git commit -m "feat: add support for X metric"
```

5. **Push & Create PR**
```bash
   git push origin feature/my-feature
   # Create PR on GitHub
```

---

## 🧪 Testing Requirements

### All PRs Must:

✅ Include tests for new functionality  
✅ Maintain or improve coverage (target: 80%+)  
✅ Pass all existing tests  
✅ Pass race detector  
✅ Follow Go best practices  

### Running Tests
```bash
# All tests
go test ./receiver -v

# With coverage
go test ./receiver -cover

# Race detector
go test ./receiver -race

# Specific test
go test ./receiver -run TestYourFeature -v
```

### Coverage Target

- Minimum: 70%
- Target: 80%
- Current: 82.3%

**Don't decrease coverage with your PR!**

---

## 📝 Code Style

### Follow Go Standards

- Use `gofmt` for formatting
- Follow [Effective Go](https://golang.org/doc/effective_go.html)
- Use meaningful variable names
- Add comments for public functions
- Keep functions small and focused

### Examples

**Good:**
```go
// GetInterval returns the configured interval or default
func (c *MetricCategoryConfig) GetInterval(defaultInterval time.Duration) time.Duration {
    if c.Interval == "" {
        return defaultInterval
    }
    // ...
}
```

**Bad:**
```go
func (c *MetricCategoryConfig) GetInterval(d time.Duration) time.Duration {
    if c.Interval=="" { return d }
    // ...
}
```

---

## 🏗 Architecture Guidelines

### Adding New Metrics

1. **Update SQL query in `client.go`**
```go
   func (c *snowflakeClient) queryNewMetric(ctx context.Context, metrics *snowflakeMetrics) error {
       query := `SELECT ...`
       // Query logic
   }
```

2. **Add transformation in `scraper.go`**
```go
   func (s *snowflakeScraper) addNewMetrics(scopeMetrics pmetric.ScopeMetrics, data []newMetricRow, now pcommon.Timestamp) {
       // Transform to OTel format
   }
```

3. **Add config in `config.go`**
```go
   type MetricsConfig struct {
       // ... existing
       NewMetric MetricCategoryConfig `mapstructure:"new_metric"`
   }
```

4. **Add tests!**
```go
   func TestClient_QueryNewMetric(t *testing.T) { /* ... */ }
   func TestScraper_AddNewMetrics(t *testing.T) { /* ... */ }
```

5. **Update documentation**
   - README.md
   - Example configs

---

## 🎯 Commit Message Convention

### Format
```
<type>(<scope>): <subject>

<body>

<footer>
```

### Types

- `feat`: New feature
- `fix`: Bug fix
- `docs`: Documentation only
- `test`: Add or update tests
- `refactor`: Code restructuring
- `perf`: Performance improvement
- `chore`: Maintenance tasks

### Examples
```
feat(metrics): add warehouse utilization metric

Add new metric to track warehouse CPU/memory utilization.
Includes SQL query, transformation, and tests.

Closes #123
```
```
fix(client): handle null values in credit usage query

Snowflake returns NULL for cloud service credits on trial accounts.
Add proper null handling to prevent panic.

Fixes #456
```
```
test(scraper): increase coverage for event tables

Add tests for event table error scenarios and edge cases.
Coverage increased from 75% to 82%.
```

---

## 📚 Documentation Updates

### When to Update Docs

- New features
- Configuration changes
- Breaking changes
- New metrics
- Bug fixes (if user-facing)

### Files to Update

- `README.md` - Main documentation
- `TESTING.md` - Test documentation
- Example configs in `config-*.yaml`
- Inline code comments

---

## 🔍 Code Review Process

### What Reviewers Look For

✅ Tests included and passing  
✅ Code follows style guide  
✅ Documentation updated  
✅ No breaking changes (or justified)  
✅ Performance impact considered  
✅ Error handling appropriate  

### Review Timeline

- Initial feedback: 2-3 days
- Follow-up: 1-2 days
- Merge: After approval + CI pass

---

## 🐛 Debugging Tips

### Common Issues

**Tests failing?**
```bash
go test ./receiver -v  # See full output
go test ./receiver -run TestFailingTest -v  # Run specific test
```

**Build errors?**
```bash
go mod tidy  # Fix dependencies
go vet ./receiver  # Check for issues
```

**Coverage decreased?**
```bash
go test ./receiver -coverprofile=coverage.out
go tool cover -func=coverage.out  # See what's uncovered
```

---

## 🏆 Recognition

Contributors will be:
- Listed in CONTRIBUTORS.md
- Credited in release notes
- Mentioned in documentation (for significant contributions)

---

## 📞 Getting Help

- Open an issue for questions
- Check existing documentation
- Review test examples
- Ask in pull request comments

---

## ⚖️ License

By contributing, you agree that your contributions will be licensed under the Apache 2.0 License.

---

## 🙏 Thank You!

Every contribution makes this project better. Whether it's:
- Reporting a bug 🐛
- Fixing a typo ✏️
- Adding a feature 🚀
- Improving tests 🧪
- Updating docs 📚

**You're making a difference!** 💪

---

**Questions?** Open an issue or reach out to maintainers.

**Happy contributing!** 🎉

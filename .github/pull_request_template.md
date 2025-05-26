# =============================================================================
# PULL REQUEST TEMPLATE
# =============================================================================
# Project: Big Data Pipeline for Diabetes Prediction
# Team: Kelompok 8 RA
# =============================================================================

## 📋 Description

<!-- Provide a brief description of the changes in this PR -->

### Type of Change

<!-- Mark the relevant option with an 'x' -->

- [ ] 🐛 Bug fix (non-breaking change which fixes an issue)
- [ ] ✨ New feature (non-breaking change which adds functionality)
- [ ] 💥 Breaking change (fix or feature that would cause existing functionality to not work as expected)
- [ ] 📚 Documentation update
- [ ] 🧪 Test improvements
- [ ] 🔧 Build/CI improvements
- [ ] ♻️ Code refactoring
- [ ] ⚡ Performance improvements
- [ ] 🔒 Security improvements

## 🔗 Related Issues

<!-- Link to related issues using #issue_number -->
Closes #
Related to #

## 📸 Screenshots (if applicable)

<!-- Add screenshots for UI changes or visual improvements -->

## 🧪 Testing

### Test Coverage

- [ ] Unit tests added/updated
- [ ] Integration tests added/updated
- [ ] End-to-end tests added/updated
- [ ] Performance tests added/updated

### Test Results

<!-- Provide test results or commands to run tests -->

```bash
# Commands to test the changes
make test-unit
make test-integration
make lint
```

### Manual Testing Checklist

- [ ] Tested locally with development environment
- [ ] Tested with Docker Compose setup
- [ ] Verified data pipeline functionality
- [ ] Checked dashboard/monitoring updates
- [ ] Validated against sample datasets

## 📊 Data Engineering Specific Checks

### Data Pipeline Changes

- [ ] ETL logic is idempotent (can be safely re-run)
- [ ] Data quality validations implemented
- [ ] Proper error handling and logging added
- [ ] Schema evolution handled appropriately
- [ ] Partitioning strategy considered

### Performance Considerations

- [ ] Spark job optimization reviewed
- [ ] Memory usage analyzed
- [ ] Execution time benchmarked
- [ ] Resource allocation appropriate

### Data Quality & Governance

- [ ] Data lineage documented
- [ ] Privacy/security considerations addressed
- [ ] Data retention policies followed
- [ ] Audit trails maintained

## 🚀 Deployment Considerations

### Environment Compatibility

- [ ] Changes work in development environment
- [ ] Compatible with staging environment
- [ ] Production deployment plan considered

### Configuration Changes

- [ ] Environment variables documented
- [ ] Configuration changes backward compatible
- [ ] Migration scripts provided (if needed)

### Monitoring & Alerting

- [ ] Relevant metrics added/updated
- [ ] Dashboard updates included
- [ ] Alert rules reviewed

## 📚 Documentation

- [ ] Code is self-documenting with clear naming
- [ ] Complex business logic has inline comments
- [ ] README updates included (if applicable)
- [ ] API documentation updated (if applicable)
- [ ] Architecture diagrams updated (if applicable)

## ✅ Code Quality Checklist

### Code Standards

- [ ] Code follows project style guidelines (PEP 8)
- [ ] Functions have appropriate docstrings
- [ ] Type hints added for new functions
- [ ] No hardcoded values (use configuration)
- [ ] Error handling implemented appropriately

### Security Review

- [ ] No sensitive data in code or logs
- [ ] Input validation implemented
- [ ] SQL injection prevention measures
- [ ] Authentication/authorization considered

### Performance Review

- [ ] Code is efficient and optimized
- [ ] No obvious performance bottlenecks
- [ ] Resource usage is reasonable
- [ ] Caching strategies implemented where appropriate

## 🔍 Review Guidelines

### For Reviewers

Please check:

1. **Functionality**: Does the code do what it's supposed to do?
2. **Data Engineering Best Practices**: Are ETL patterns followed correctly?
3. **Performance**: Will this scale with larger datasets?
4. **Security**: Are there any security implications?
5. **Maintainability**: Is the code easy to understand and modify?
6. **Testing**: Is the code adequately tested?

### Areas of Focus

<!-- Highlight specific areas where you want reviewer attention -->

- [ ] Algorithm implementation
- [ ] Data transformation logic
- [ ] Performance optimization
- [ ] Error handling
- [ ] Security considerations
- [ ] Documentation quality

## 📝 Additional Notes

<!-- Any additional information that reviewers should know -->

### Breaking Changes

<!-- If this is a breaking change, document what breaks and how to migrate -->

### Migration Steps

<!-- If database or data migrations are needed -->

### Rollback Plan

<!-- How to rollback if something goes wrong -->

---

## 🎯 Post-Merge Checklist

<!-- Things to do after the PR is merged -->

- [ ] Update project documentation
- [ ] Notify team of changes
- [ ] Monitor deployment in staging
- [ ] Plan production deployment
- [ ] Update training materials (if needed)

---

**Reviewer**: @username
**Estimated Review Time**: X hours
**Merge Timeline**: Before [date]

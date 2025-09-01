# Technical Assignment: High-Performance GitHub Repository Crawler

## Overview
Build a high-performance GitHub repository crawler that can extract repository data at scale using GitHub's GraphQL API.

## Core Requirements

### Performance Target
- Extract **at least 100,000 unique repositories** in **less than 10 minutes**
- Use **only one GitHub token** (the default CI token provided)
- Store data in a PostgreSQL database with efficient schema design

### Data Requirements
- Crawl basic repository details (name, description, stars, etc.)
- Ensure data uniqueness across crawled repositories
- Design for future metadata expansion (issues, pull requests, etc.)

### Technical Constraints
- Respect GitHub API rate limits and implement proper retry mechanisms
- Use clean architecture principles and enterprise-grade software engineering practices
- Implement comprehensive error handling and logging

## Deliverables

### 1. Working System
- Complete crawler implementation with proper architecture
- PostgreSQL database integration with optimized schema
- GitHub Actions CI/CD pipeline that demonstrates the system working

### 2. CI Pipeline Requirements
Your GitHub Actions pipeline must include:
- PostgreSQL service container setup
- Database schema creation and optimization
- Repository crawling execution (100K+ repos in <10 minutes)
- Data export as artifacts (JSON format preferred)
- Must work with single default GitHub token (no admin privileges required)

### 3. Architecture Documentation
- Explain your approach to achieving the performance requirements
- Document your database schema design decisions
- Describe how you handle rate limiting and error scenarios

### 4. Scaling Strategy
**Critical Question**: How would you evolve this system to handle **500 million repositories**?
- What architectural changes would be needed?
- How would the database schema evolve for additional metadata?
- What infrastructure considerations come into play?

## Evaluation Criteria

### Performance (High Priority)
- Ability to meet the 100K repositories in <10 minutes requirement
- Efficient use of GitHub API (rate limit optimization)
- Database performance and query efficiency

### Code Quality (High Priority)
- Clean architecture implementation
- Separation of concerns and modularity
- Error handling and resilience
- Testing coverage

### Scalability Thinking (Medium Priority)
- Thoughtful approach to the 500M scaling question
- Database design that supports future growth
- Understanding of distributed systems concepts

## Bonus Points
- Simple demo UI showing crawled data (not required, but appreciated)
- Creative approaches to performance optimization
- Innovative solutions to common crawling challenges

## Submission
Submit your solution in this repository with:
1. Complete source code
2. Successful GitHub Actions run demonstrating the system
3. README with setup instructions and architecture explanation
4. Documentation of your 500M scaling strategy

---

**Note**: This assignment is intentionally somewhat open-ended. We encourage you to make reasonable assumptions and ask clarifying questions. Your approach to handling ambiguity is part of the evaluation.

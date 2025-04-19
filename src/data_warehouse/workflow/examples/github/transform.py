"""
GitHub Data Transformation Module.

This module provides classes for transforming GitHub API data.
"""

from datetime import datetime
from typing import Any

from loguru import logger

from data_warehouse.core.exceptions import TransformerError
from data_warehouse.workflow.base import WorkflowContext
from data_warehouse.workflow.etl import TransformerBase


class GitHubTransformer(TransformerBase[list[dict[str, Any]], list[dict[str, Any]]]):
    """Transformer for GitHub API data."""

    def transform(self, data: list[dict[str, Any]], context: WorkflowContext) -> list[dict[str, Any]]:
        """Transform GitHub API data.

        Args:
            data: The GitHub API data to transform
            context: The workflow context

        Returns:
            Transformed data

        Raises:
            TransformerError: If transformation fails
        """
        if not data:
            return []

        context.config.get("entity_type", "repo")
        endpoint = context.config.get("endpoint", "issues")

        logger.info(f"Transforming {len(data)} GitHub {endpoint} items")

        try:
            # Select appropriate transformation method based on endpoint
            if endpoint == "issues":
                return self._transform_issues(data)
            elif endpoint == "pulls":
                return self._transform_pull_requests(data)
            elif endpoint == "commits":
                return self._transform_commits(data)
            elif endpoint == "repositories":
                return self._transform_repos(data)
            elif endpoint == "users":
                return self._transform_users(data)
            else:
                # Default transformation for other endpoints
                return self._transform_generic(data)

        except Exception as e:
            logger.error(f"Failed to transform GitHub data: {str(e)}")
            raise TransformerError(f"Failed to transform GitHub data: {str(e)}") from e

    def _transform_generic(self, data: list[dict[str, Any]]) -> list[dict[str, Any]]:
        """Generic transformation for GitHub data.

        Args:
            data: The GitHub data to transform

        Returns:
            Transformed data
        """
        transformed = []

        for item in data:
            # Apply generic transformations
            transformed_item = {}

            # Copy essential ID and metadata fields
            if "id" in item:
                transformed_item["id"] = item["id"]
            if "url" in item:
                transformed_item["api_url"] = item["url"]
            if "html_url" in item:
                transformed_item["html_url"] = item["html_url"]
            if "created_at" in item:
                transformed_item["created_at"] = self._parse_github_date(item["created_at"])
            if "updated_at" in item:
                transformed_item["updated_at"] = self._parse_github_date(item["updated_at"])

            # Copy the rest of the fields
            for key, value in item.items():
                if key not in transformed_item:
                    transformed_item[key] = value

            transformed.append(transformed_item)

        return transformed

    def _transform_issues(self, data: list[dict[str, Any]]) -> list[dict[str, Any]]:
        """Transform GitHub issues data.

        Args:
            data: The GitHub issues data to transform

        Returns:
            Transformed issues data
        """
        transformed = []

        for issue in data:
            transformed_issue = {
                "id": issue["id"],
                "issue_number": issue["number"],
                "title": issue["title"],
                "state": issue["state"],
                "api_url": issue["url"],
                "html_url": issue["html_url"],
                "created_at": self._parse_github_date(issue["created_at"]),
                "updated_at": self._parse_github_date(issue["updated_at"]),
                "closed_at": self._parse_github_date(issue["closed_at"]) if issue.get("closed_at") else None,
                "body": issue.get("body", ""),
                "comments_count": issue.get("comments", 0),
                "is_pull_request": "pull_request" in issue,
                "labels": [label["name"] for label in issue.get("labels", [])],
                "assignees": [user["login"] for user in issue.get("assignees", [])],
                "milestone": issue["milestone"]["title"] if issue.get("milestone") else None,
                "author": issue["user"]["login"] if issue.get("user") else None,
            }

            transformed.append(transformed_issue)

        return transformed

    def _transform_pull_requests(self, data: list[dict[str, Any]]) -> list[dict[str, Any]]:
        """Transform GitHub pull requests data.

        Args:
            data: The GitHub pull requests data to transform

        Returns:
            Transformed pull requests data
        """
        transformed = []

        for pr in data:
            transformed_pr = {
                "id": pr["id"],
                "pr_number": pr["number"],
                "title": pr["title"],
                "state": pr["state"],
                "api_url": pr["url"],
                "html_url": pr["html_url"],
                "created_at": self._parse_github_date(pr["created_at"]),
                "updated_at": self._parse_github_date(pr["updated_at"]),
                "closed_at": self._parse_github_date(pr["closed_at"]) if pr.get("closed_at") else None,
                "merged_at": self._parse_github_date(pr["merged_at"]) if pr.get("merged_at") else None,
                "body": pr.get("body", ""),
                "comments_count": pr.get("comments", 0),
                "review_comments_count": pr.get("review_comments", 0),
                "labels": [label["name"] for label in pr.get("labels", [])],
                "assignees": [user["login"] for user in pr.get("assignees", [])],
                "milestone": pr["milestone"]["title"] if pr.get("milestone") else None,
                "author": pr["user"]["login"] if pr.get("user") else None,
                "is_draft": pr.get("draft", False),
                "is_merged": pr.get("merged", False),
                "base_branch": pr.get("base", {}).get("ref"),
                "head_branch": pr.get("head", {}).get("ref"),
            }

            transformed.append(transformed_pr)

        return transformed

    def _transform_commits(self, data: list[dict[str, Any]]) -> list[dict[str, Any]]:
        """Transform GitHub commits data.

        Args:
            data: The GitHub commits data to transform

        Returns:
            Transformed commits data
        """
        transformed = []

        for commit in data:
            # Extract author information
            author_name = None
            author_email = None
            author_date = None

            if "commit" in commit:
                if "author" in commit["commit"]:
                    author_name = commit["commit"]["author"].get("name")
                    author_email = commit["commit"]["author"].get("email")
                    author_date = self._parse_github_date(commit["commit"]["author"].get("date"))

            # Extract committer information
            committer_name = None
            committer_email = None
            committer_date = None

            if "commit" in commit:
                if "committer" in commit["commit"]:
                    committer_name = commit["commit"]["committer"].get("name")
                    committer_email = commit["commit"]["committer"].get("email")
                    committer_date = self._parse_github_date(commit["commit"]["committer"].get("date"))

            transformed_commit = {
                "sha": commit["sha"],
                "api_url": commit["url"],
                "html_url": commit.get("html_url"),
                "message": commit.get("commit", {}).get("message", ""),
                "author_name": author_name,
                "author_email": author_email,
                "author_date": author_date,
                "committer_name": committer_name,
                "committer_email": committer_email,
                "committer_date": committer_date,
                "github_author": commit.get("author", {}).get("login") if commit.get("author") else None,
                "github_committer": commit.get("committer", {}).get("login") if commit.get("committer") else None,
                "parents": [parent["sha"] for parent in commit.get("parents", [])],
                "stats": commit.get("stats", {}),
            }

            transformed.append(transformed_commit)

        return transformed

    def _transform_repos(self, data: list[dict[str, Any]]) -> list[dict[str, Any]]:
        """Transform GitHub repositories data.

        Args:
            data: The GitHub repositories data to transform

        Returns:
            Transformed repositories data
        """
        transformed = []

        for repo in data:
            transformed_repo = {
                "id": repo["id"],
                "name": repo["name"],
                "full_name": repo["full_name"],
                "api_url": repo["url"],
                "html_url": repo["html_url"],
                "description": repo.get("description", ""),
                "created_at": self._parse_github_date(repo["created_at"]),
                "updated_at": self._parse_github_date(repo["updated_at"]),
                "pushed_at": self._parse_github_date(repo["pushed_at"]) if repo.get("pushed_at") else None,
                "homepage": repo.get("homepage"),
                "language": repo.get("language"),
                "fork": repo.get("fork", False),
                "forks_count": repo.get("forks_count", 0),
                "stargazers_count": repo.get("stargazers_count", 0),
                "watchers_count": repo.get("watchers_count", 0),
                "open_issues_count": repo.get("open_issues_count", 0),
                "topics": repo.get("topics", []),
                "visibility": repo.get("visibility", "public"),
                "default_branch": repo.get("default_branch", "main"),
                "owner": repo["owner"]["login"] if repo.get("owner") else None,
                "is_archived": repo.get("archived", False),
                "is_disabled": repo.get("disabled", False),
            }

            transformed.append(transformed_repo)

        return transformed

    def _transform_users(self, data: list[dict[str, Any]]) -> list[dict[str, Any]]:
        """Transform GitHub users data.

        Args:
            data: The GitHub users data to transform

        Returns:
            Transformed users data
        """
        transformed = []

        for user in data:
            transformed_user = {
                "id": user["id"],
                "login": user["login"],
                "api_url": user["url"],
                "html_url": user["html_url"],
                "type": user.get("type", "User"),
                "name": user.get("name"),
                "company": user.get("company"),
                "blog": user.get("blog"),
                "location": user.get("location"),
                "email": user.get("email"),
                "bio": user.get("bio"),
                "twitter_username": user.get("twitter_username"),
                "public_repos": user.get("public_repos", 0),
                "public_gists": user.get("public_gists", 0),
                "followers": user.get("followers", 0),
                "following": user.get("following", 0),
                "created_at": self._parse_github_date(user["created_at"]) if "created_at" in user else None,
                "updated_at": self._parse_github_date(user["updated_at"]) if "updated_at" in user else None,
                "is_site_admin": user.get("site_admin", False),
            }

            transformed.append(transformed_user)

        return transformed

    @staticmethod
    def _parse_github_date(date_str: str | None) -> datetime | None:
        """Parse GitHub ISO 8601 date string to datetime.

        Args:
            date_str: The date string to parse

        Returns:
            Parsed datetime or None if parsing fails
        """
        if not date_str:
            return None

        try:
            return datetime.fromisoformat(date_str.replace("Z", "+00:00"))
        except ValueError:
            logger.warning(f"Failed to parse date: {date_str}")
            return None

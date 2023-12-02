### Workflow to deploy documentation from gh-pages

1. Write/update docs in main or gh-pages
2. Commit and push changes
3. Switch to `gh-pages`
    ```bash
    git checkout gh-pages
    ```
4. Merge changes from `main`
    ```bash
    git checkout main -- docs
    ```
5. Commit and push changes from `gh-pages` local to remote
    ```bash
    # Add the changes
    git add .

    # Commit the changes
    git commit -m "Merge documentation changes"

    # Push the changes
    git push origin gh-pages
    ```
6. Set up GitHub Pages (one-time action)
    - go to GitHub > settings > pages
    - select `gh-pages` as branch
    - select `docs/` as folder

7. GitHub Actions automatically publishes new GitHub Pages after changes are pushed to github

Documentation is published here: [Urban Climate Documentation](https://ac-willeke.github.io/urban-climate/).

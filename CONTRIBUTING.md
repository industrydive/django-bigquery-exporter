# CONTRIBUTING to django-bigquery-exporter

First off, thank you for considering contributing to django-bigquery-exporter! So much software depends on the thought and generosity of folks like you. We welcome any type of contribution, not only code.

## 1. Where do I go from here?

If you've noticed a bug or have a question, [search the issue tracker](https://github.com/industrydive/django-bigquery-exporter/issues) to see if someone else has already created a ticket. If not, go ahead and [make one](https://github.com/yourusername/django-bigquery-exporter/issues/new)!

## 2. Fork & create a branch

If this is something you think you can fix, then [fork django-bigquery-exporter](https://help.github.com/articles/fork-a-repo) and create a branch with a descriptive name.

Please prefix your branch name with OSC-<issue number>, e.g., if the issue number were 827, your branch might be called:

```shell
git checkout -b OSC-827-add-japanese-locale
```

## 3. Did you find a bug?

### Ensure the bug was not already reported

**IMPORTANT**: Before filing a bug, ensure it was not already reported by searching on GitHub under [Issues](https://github.com/industrydive/django-bigquery-exporter/issues).

If you're unable to find an open issue addressing the problem, [open a new one](https://github.com/industrydive/django-bigquery-exporter/issues/new). Be sure to include a **title and clear description**, as much relevant information as possible, and a **code sample** or an **executable test case** demonstrating the expected behavior that is not occurring.

### Did you write a patch that fixes a bug?

Open a new GitHub pull request with the patch.

Ensure the PR description clearly describes the problem and solution. Include the relevant issue number if applicable.

## 4. Implement your fix or feature

At this point, you're ready to make your changes! Feel free to ask for help; everyone is a beginner at first.

## 5. Get the code

The first thing you'll need to do is get the django-bigquery-exporter code. 

```shell
# Clone your fork of the repository
git clone https://github.com/industrydive/django-bigquery-exporter.git
```

## 6. Test your changes

Ensure that your changes pass both the unit and integration tests. 

```shell
# Run the tests
python manage.py test
```

## 7. Make a pull request

At this point, you should switch back to your main branch and make sure it's up to date with django-bigquery-exporter's main branch:

```shell
git remote add upstream https://github.com/industrydive/django-bigquery-exporter.git
git checkout main
git pull upstream main
```

Then update your feature branch from your local copy of main, and push it!

```shell
git checkout OSC-827-add-japanese-locale
git rebase main
git push --set-upstream origin OSC-827-add-japanese-locale
```

Finally, go to GitHub and [make a Pull Request](https://github.com/industrydive/django-bigquery-exporter/pulls) :D

## 8. Keeping your Pull Request updated

If a maintainer asks you to "rebase" your PR, they're saying that a lot of code has changed, and that you need to update your branch so it's easier to merge.

## 9. Conduct

We have a code of conduct - please follow it in all your interactions with the project.

## 10. Where can I ask for help?

You can ask for help in the project's [issue tracker](https://github.com/industrydive/django-bigquery-exporter/issues). Alternatively, you can ask for help on software development or open-source related communities, forums, or social media groups.

## 11. What about if I have something else in mind?

If you have something else in mind, propose it through an issue.

Remember, contributions are not just about code. There are a number of ways you can contribute to the project:

* **Improving documentation**: If you notice any mistakes, ambiguities or missing information in the documentation, you are welcome to edit and improve it.
* **Reporting issues**: Just like with code contributions, you can make a significant impact on this project by reporting issues. Please make sure to include as much detail as you can, including how to reproduce the issue.
* **Testing**: If you'd like to help with testing, that's great! You can assist by confirming bug reports and performing manual testing of new features or changes.
* **Promoting**: If you don't have the time to contribute directly or it's not really your thing, you can still help by promoting django-bigquery-exporter, writing about it, or introducing it to others.

Please adhere to this project's `Code of Conduct` in all your interactions.

Remember: **Every contribution counts!** Your help is valuable and appreciated.


Again, thank you for your interest in contributing to django-bigquery-exporter! We look forward to your contribution.

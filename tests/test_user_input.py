from __future__ import annotations

import pytest

from dsh_base_agent import UserQuestion
from dsh_base_agent.sdk.user_input import validate_user_input_response


def test_free_text_and_selected_answers_are_validated_against_questions() -> None:
    questions = [
        {
            "id": "shipping",
            "question": "Choose shipping.",
            "options": [{"label": "standard"}, {"label": "express"}],
        },
        {"id": "note", "question": "Add a delivery note."},
    ]

    response = validate_user_input_response(
        {
            "answers": [
                {"id": "shipping", "selected": ["express"]},
                {"id": "note", "custom": "Leave at reception."},
            ]
        },
        questions,
    )

    assert response.answers[0].selected == ("express",)
    assert response.answers[1].custom == "Leave at reception."


def test_answer_must_cover_the_exact_question_set() -> None:
    with pytest.raises(ValueError, match="every requested question"):
        validate_user_input_response(
            {"answers": [{"id": "another", "custom": "value"}]},
            [{"id": "expected", "question": "Answer me."}],
        )


def test_answer_rejects_unknown_or_multiple_single_select_options() -> None:
    question = {
        "id": "shipping",
        "question": "Choose shipping.",
        "options": [{"label": "standard"}, {"label": "express"}],
    }
    with pytest.raises(ValueError, match="unknown option"):
        validate_user_input_response(
            {"answers": [{"id": "shipping", "selected": ["overnight"]}]},
            [question],
        )
    with pytest.raises(ValueError, match="only one"):
        validate_user_input_response(
            {"answers": [{"id": "shipping", "selected": ["standard", "express"]}]},
            [question],
        )


def test_multi_select_requires_options() -> None:
    with pytest.raises(ValueError, match="multi_select requires"):
        UserQuestion(
            id="features",
            question="Choose features.",
            multi_select=True,
        )

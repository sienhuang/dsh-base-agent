"""Validated model-facing user-question and human-answer contracts."""

from __future__ import annotations

from typing import Any

from pydantic import BaseModel, ConfigDict, Field, model_validator

ASK_USER_TOOL_NAME = "ask_user_question"


class UserQuestionOption(BaseModel):
    model_config = ConfigDict(extra="forbid")

    label: str = Field(min_length=1, max_length=100)
    description: str | None = Field(default=None, max_length=500)


class UserQuestion(BaseModel):
    model_config = ConfigDict(extra="forbid")

    id: str = Field(min_length=1, max_length=64, pattern=r"^[A-Za-z0-9_.-]+$")
    question: str = Field(min_length=1, max_length=1_000)
    header: str | None = Field(default=None, max_length=64)
    options: tuple[UserQuestionOption, ...] = Field(default=(), max_length=10)
    multi_select: bool = False

    @model_validator(mode="after")
    def validate_options(self) -> UserQuestion:
        labels = [item.label for item in self.options]
        if len(labels) != len(set(labels)):
            raise ValueError("question option labels must be unique")
        if self.multi_select and not self.options:
            raise ValueError("multi_select requires at least one option")
        return self


class AskUserQuestionRequest(BaseModel):
    model_config = ConfigDict(extra="forbid")

    questions: tuple[UserQuestion, ...] = Field(min_length=1, max_length=3)

    @model_validator(mode="after")
    def validate_question_ids(self) -> AskUserQuestionRequest:
        identifiers = [item.id for item in self.questions]
        if len(identifiers) != len(set(identifiers)):
            raise ValueError("question ids must be unique")
        return self


class UserAnswer(BaseModel):
    model_config = ConfigDict(extra="forbid")

    id: str = Field(min_length=1, max_length=64)
    selected: tuple[str, ...] = ()
    custom: str | None = Field(default=None, min_length=1, max_length=4_000)


class UserInputResponse(BaseModel):
    model_config = ConfigDict(extra="forbid")

    answers: tuple[UserAnswer, ...] = Field(min_length=1, max_length=3)


def validate_user_input_response(
    payload: dict[str, Any],
    questions_payload: object,
) -> UserInputResponse:
    """Validate an answer against the exact immutable questions that were shown."""

    request = AskUserQuestionRequest.model_validate({"questions": questions_payload})
    response = UserInputResponse.model_validate(payload)
    questions = {item.id: item for item in request.questions}
    answers = {item.id: item for item in response.answers}
    if len(answers) != len(response.answers):
        raise ValueError("answer ids must be unique")
    missing = set(questions) - set(answers)
    extra = set(answers) - set(questions)
    if missing or extra:
        raise ValueError("answers must match every requested question exactly")
    for identifier, answer in answers.items():
        question = questions[identifier]
        selected = list(answer.selected)
        if len(selected) != len(set(selected)):
            raise ValueError(f"answer '{identifier}' contains duplicate selections")
        allowed = {item.label for item in question.options}
        if any(label not in allowed for label in selected):
            raise ValueError(f"answer '{identifier}' selected an unknown option")
        if not question.multi_select and len(selected) > 1:
            raise ValueError(f"answer '{identifier}' allows only one selected option")
        if not selected and answer.custom is None:
            raise ValueError(f"answer '{identifier}' must select an option or provide custom text")
    return response


__all__ = [
    "ASK_USER_TOOL_NAME",
    "AskUserQuestionRequest",
    "UserAnswer",
    "UserInputResponse",
    "UserQuestion",
    "UserQuestionOption",
    "validate_user_input_response",
]

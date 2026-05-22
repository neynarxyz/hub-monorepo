import { Message, MessageType, UserDataType } from "@farcaster/hub-nodejs";
import { convertProtobufMessageBodyToJson, scrubStringFieldsForPostgresJson } from "./utils";

// Constructs a barely-valid Message-shaped object for routing through the
// body builder. We don't go through protobuf encode/decode here: protobuf's
// own string handling is what introduced the bad bytes upstream, and the
// only thing we're testing is what `convertProtobufMessageBodyToJson` does
// once a JS string with bad chars is in hand. Casting to Message is
// sufficient because the function only inspects `message.data`.
function userDataAddMessage(value: string, subtype: UserDataType = UserDataType.BIO): Message {
  return {
    data: {
      type: MessageType.USER_DATA_ADD,
      userDataBody: { type: subtype, value },
    },
  } as unknown as Message;
}

function castAddMessage(text: string): Message {
  return {
    data: {
      type: MessageType.CAST_ADD,
      castAddBody: {
        embeds: [],
        mentions: [],
        mentionsPositions: [],
        text,
        parentCastId: undefined,
        parentUrl: undefined,
        type: 0,
      },
    },
  } as unknown as Message;
}

describe("scrubStringFieldsForPostgresJson", () => {
  test("strips NUL bytes from plain strings", () => {
    expect(scrubStringFieldsForPostgresJson("hello\u0000world")).toBe("helloworld");
  });

  test("strips lone high surrogates", () => {
    expect(scrubStringFieldsForPostgresJson("a\uD800b")).toBe("ab");
  });

  test("strips lone low surrogates", () => {
    expect(scrubStringFieldsForPostgresJson("a\uDC00b")).toBe("ab");
  });

  test("preserves paired surrogates (astral characters)", () => {
    // U+1F600 = "😀" = "😀"
    expect(scrubStringFieldsForPostgresJson("emoji 😀 ok")).toBe("emoji 😀 ok");
  });

  test("walks into nested objects", () => {
    const input = { a: "x\u0000y", nested: { b: ["safe", "z\uD800"] } };
    expect(scrubStringFieldsForPostgresJson(input)).toEqual({
      a: "xy",
      nested: { b: ["safe", "z"] },
    });
  });

  test("walks into arrays", () => {
    expect(scrubStringFieldsForPostgresJson(["a\u0000", { c: "b\uDC00" }])).toEqual(["a", { c: "b" }]);
  });

  test("leaves non-string scalars untouched", () => {
    expect(scrubStringFieldsForPostgresJson(42)).toBe(42);
    expect(scrubStringFieldsForPostgresJson(true)).toBe(true);
    expect(scrubStringFieldsForPostgresJson(null)).toBe(null);
  });

  test("is idempotent", () => {
    const dirty = { a: "x\u0000y\uD800z\uDC00", arr: ["safe", "p\u0000q"] };
    const once = scrubStringFieldsForPostgresJson(dirty);
    const twice = scrubStringFieldsForPostgresJson(once);
    expect(twice).toEqual(once);
  });

  test("scrubbed output is castable through PG-shaped round-trip", () => {
    // The actual end-to-end constraint we care about: anything we emit
    // must survive JSON.stringify + JSON.parse without introducing
    // sequences that PG's `jsonb` parser will later reject. This catches
    // any future regression where the scrub leaves a half-cleaned surrogate.
    const dirty = { v: "lone-low \uDC00 and nul \u0000 here" };
    const clean = scrubStringFieldsForPostgresJson(dirty);
    const serialized = JSON.stringify(clean);
    expect(serialized).not.toMatch(/\\u0000/);
    expect(serialized).not.toMatch(/\\uD[89ABab][0-9A-Fa-f]{2}/);
    expect(serialized).not.toMatch(/\\uD[CDEFcdef][0-9A-Fa-f]{2}/);
  });
});

describe("convertProtobufMessageBodyToJson applies scrub", () => {
  test("USER_DATA_ADD strips NUL from value", () => {
    const body = convertProtobufMessageBodyToJson(userDataAddMessage("bio\u0000text"));
    expect(body).toEqual({ type: UserDataType.BIO, value: "biotext" });
  });

  test("USER_DATA_ADD strips lone surrogates", () => {
    const body = convertProtobufMessageBodyToJson(userDataAddMessage("a\uD800b\uDC00c"));
    expect(body).toEqual({ type: UserDataType.BIO, value: "abc" });
  });

  test("CAST_ADD strips NUL from text", () => {
    const body = convertProtobufMessageBodyToJson(castAddMessage("hello\u0000"));
    // biome-ignore lint/suspicious/noExplicitAny: structural assertion against the body union
    expect((body as any).text).toBe("hello");
  });

  test("CAST_ADD preserves paired surrogates (emoji)", () => {
    const body = convertProtobufMessageBodyToJson(castAddMessage("hi 😀"));
    // biome-ignore lint/suspicious/noExplicitAny: structural assertion against the body union
    expect((body as any).text).toBe("hi 😀");
  });
});

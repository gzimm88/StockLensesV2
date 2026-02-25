import React from "react";
import { render, screen } from "@testing-library/react";
import AttributionPanel from "./AttributionPanel";

describe("AttributionPanel", () => {
  it("renders attribution summary and table from mocked response", () => {
    const attribution = {
      previous_nav: 100,
      current_nav: 120,
      total_explained_delta: 20,
      unexplained_delta: 0,
      transaction_delta: 5,
      price_delta: 10,
      fx_delta: 3,
      corporate_action_delta: 2,
      breakdown_by_ticker: {
        AAPL: {
          transaction_delta: 5,
          price_delta: 10,
          fx_delta: 3,
          corporate_action_delta: 2,
          total_delta: 20,
        },
      },
    };

    render(<AttributionPanel attribution={attribution} diff={null} />);

    expect(screen.getByTestId("attribution-panel")).toBeInTheDocument();
    expect(screen.getByText("Previous NAV")).toBeInTheDocument();
    expect(screen.getByText("Current NAV")).toBeInTheDocument();
    expect(screen.getByText("Total Explained Delta")).toBeInTheDocument();
    expect(screen.getByText("Unexplained Delta")).toBeInTheDocument();
    expect(screen.getByText("AAPL")).toBeInTheDocument();
    expect(screen.getAllByText("20").length).toBeGreaterThan(0);
  });

  it("renders integrity error banner when unexplained_delta is non-zero", () => {
    render(
      <AttributionPanel
        attribution={{
          unexplained_delta: 0.01,
          breakdown_by_ticker: {},
        }}
        diff={null}
      />
    );

    expect(screen.getByRole("alert")).toHaveTextContent("Attribution integrity check failed.");
  });
});

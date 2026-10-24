import { Box, Button, HStack, Link, Text } from "@chakra-ui/react";

import { healerPageHref } from "src/links";

/** Small header of the standalone page: the title (a link to the Healer page), the user, Sign out. */
export const StandaloneHeader = ({
  onSignOut,
  title,
  user,
}: {
  readonly onSignOut?: () => void;
  readonly title: string;
  readonly user?: string;
}) => (
  <Box
    as="header"
    bg="bg.subtle"
    borderBottomWidth="1px"
    borderColor="border"
    data-testid="standalone-header"
    px={{ base: 4, md: 6 }}
    py={3}
  >
    <HStack gap={4} justifyContent="space-between" wrap="wrap">
      <Link color="fg" fontWeight="semibold" href={healerPageHref()} variant="plain">
        {title}
      </Link>
      <HStack gap={3}>
        {user === undefined ? undefined : (
          <Text color="fg.muted" data-testid="standalone-user" fontSize="sm">
            {user}
          </Text>
        )}
        {onSignOut === undefined ? undefined : (
          <Button onClick={onSignOut} size="xs" variant="outline">
            Sign out
          </Button>
        )}
      </HStack>
    </HStack>
  </Box>
);
